// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Autofac;
using Nethermind.Api;
using Nethermind.Config;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Specs;
using Nethermind.Core.Test.Builders;
using Nethermind.Core.Test.Modules;
using Nethermind.Db;
using Nethermind.Db.Rocks;
using Nethermind.Db.Rocks.Config;
using Nethermind.Evm.State;
using Nethermind.Int256;
using Nethermind.Specs.Forks;
using Nethermind.State;
using Nethermind.Logging;
using Nethermind.Trie;
using Nethermind.Trie.Pruning;

namespace Nethermind.Benchmark.Runner;

public class MptBench
{
    public static void Run(int nAccounts = 100, int nSlots = 1000, int mModify = 10, int kCommit = 50, string dbPath = "mpt_bench_db")
    {
        string fullPath = Path.GetFullPath(dbPath);
        if (Directory.Exists(fullPath))
        {
            Console.WriteLine($"Cleaning up old database at {fullPath}...");
            Directory.Delete(fullPath, true);
        }

        Directory.CreateDirectory(fullPath);

        Console.WriteLine($"Initializing RocksDB at {fullPath}...");
        ConfigProvider configProvider = new();
        IInitConfig initConfig = configProvider.GetConfig<IInitConfig>();
        initConfig.BaseDbPath = fullPath;
        initConfig.DataDir = fullPath;
        initConfig.DiagnosticMode = DiagnosticMode.None;
        
        // 使用 Nethermind 的 HalfPath 方案
        initConfig.StateDbKeyScheme = INodeStorage.KeyScheme.HalfPath;

        IPruningConfig pruningConfig = configProvider.GetConfig<IPruningConfig>();
        pruningConfig.Mode = PruningMode.Hybrid; 
        pruningConfig.PersistenceInterval = 1;
        pruningConfig.DirtyCacheMb = 2048; // 2GB Cache
        pruningConfig.PruningBoundary = 64; 

        IDbConfig dbConfig = configProvider.GetConfig<IDbConfig>();
        dbConfig.WriteAheadLogSync = false; 
        
        // 极致性能 RocksDB 配置
        dbConfig.RocksDbOptions = "compression=kNoCompression;bottommost_compression=kNoCompression;" + 
                                  "write_buffer_size=536870912;max_write_buffer_number=6;min_write_buffer_number_to_merge=2;" +
                                  "target_file_size_base=134217728;max_bytes_for_level_base=1073741824;" +
                                  "block_based_table_factory={filter_policy=bloomfilter:10:false;block_cache=1073741824;};" +
                                  "max_background_compactions=8;max_background_flushes=6;";

        var container = new ContainerBuilder()
            .AddModule(new TestNethermindModule(configProvider))
            .AddSingleton<IRocksDbConfigFactory, RocksDbConfigFactory>()
            .AddSingleton<IDbFactory, RocksDbFactory>()
            .Build();

        IWorldStateManager worldStateManager = container.Resolve<IWorldStateManager>();
        IWorldState worldState = worldStateManager.GlobalWorldState;
        IReleaseSpec releaseSpec = new Prague();

        Console.WriteLine($"Phase 1: Creating {nAccounts} accounts with variable slots (avg {nSlots}, Scheme: HalfPath)...");
        Stopwatch sw = Stopwatch.StartNew();

        Address[] addrs = new Address[nAccounts];
        Hash256 currentRoot = Hash256.Zero;
        BlockHeader currentHeader = IWorldState.PreGenesis;
        Random rand = new Random(42);
        byte[] valBuffer = new byte[32];

        for (int batchStart = 0; batchStart < nAccounts; batchStart += kCommit)
        {
            int batchEnd = Math.Min(batchStart + kCommit, nAccounts);
            
            using (worldState.BeginScope(currentHeader))
            {
                for (int i = batchStart; i < batchEnd; i++)
                {
                    byte[] addrBytes = Keccak.Compute(System.Text.Encoding.UTF8.GetBytes($"account-{i}")).Bytes.Slice(0, 20).ToArray();
                    Address addr = new Address(addrBytes);
                    addrs[i] = addr;

                    worldState.AddToBalanceAndCreateIfNotExists(addr, (UInt256)1e18, releaseSpec);
                    worldState.SetNonce(addr, (UInt256)i);

                    int variableSlots = rand.Next(nSlots * 2);
                    for (int j = 0; j < variableSlots; j++)
                    {
                        UInt256 slotKey = new UInt256(Keccak.Compute(System.Text.Encoding.UTF8.GetBytes($"acc-{i}-slot-{j}")).Bytes);
                        
                        int dice = rand.Next(100);
                        if (dice < 20) Array.Clear(valBuffer, 0, 32);
                        else if (dice < 30) { Array.Clear(valBuffer, 0, 32); valBuffer[31] = 1; }
                        else rand.NextBytes(valBuffer);

                        worldState.Set(new StorageCell(addr, slotKey), valBuffer.ToArray());
                    }

                    if ((i + 1) % 10 == 0 || i + 1 == nAccounts)
                    {
                        Console.Write($"\r...processed {i + 1}/{nAccounts} accounts");
                    }
                }

                worldState.Commit(releaseSpec);
                worldState.CommitTree(batchStart / kCommit);
                currentRoot = worldState.StateRoot;
                currentHeader = Build.A.BlockHeader.WithStateRoot(currentRoot).TestObject;
                worldState.Reset();
            }

            worldStateManager.FlushCache(CancellationToken.None);
            GC.Collect(2, GCCollectionMode.Forced, true);
            
            Console.WriteLine($"\n[Batch {(batchStart / kCommit) + 1}] Root: {currentRoot.ToShortString()} | Disk: {(double)GetDirSize(fullPath) / (1024 * 1024):F2} MB | Memory: {Process.GetCurrentProcess().WorkingSet64 / 1024.0 / 1024.0:F2} MB");
        }

        Console.WriteLine($"\nCreation finished in {sw.Elapsed}. Final Root: {currentRoot}");

        // Phase 2: Modification
        mModify = Math.Min(mModify, nAccounts);
        Console.WriteLine($"Phase 2: Randomly modifying slots in {mModify} accounts...");
        sw.Restart();

        int[] perm = Enumerable.Range(0, nAccounts).OrderBy(x => rand.Next()).ToArray();

        for (int batchStart = 0; batchStart < mModify; batchStart += kCommit)
        {
            int batchEnd = Math.Min(batchStart + kCommit, mModify);

            using (worldState.BeginScope(currentHeader))
            {
                for (int i = batchStart; i < batchEnd; i++)
                {
                    int accountIdx = perm[i];
                    Address addr = addrs[accountIdx];

                    for (int j = 0; j < 100; j++)
                    {
                        int slotIdx = rand.Next(nSlots); 
                        UInt256 slotKey = new UInt256(Keccak.Compute(System.Text.Encoding.UTF8.GetBytes($"acc-{accountIdx}-slot-{slotIdx}")).Bytes);
                        rand.NextBytes(valBuffer);
                        worldState.Set(new StorageCell(addr, slotKey), valBuffer.ToArray());
                    }
                }

                worldState.Commit(releaseSpec);
                worldState.CommitTree((batchStart / kCommit) + 1000000);
                currentRoot = worldState.StateRoot;
                currentHeader = Build.A.BlockHeader.WithStateRoot(currentRoot).TestObject;
                worldState.Reset();
            }
            
            worldStateManager.FlushCache(CancellationToken.None);
            GC.Collect();
            Console.WriteLine($"\n[Mod Batch] Committed. Disk: {(double)GetDirSize(fullPath) / (1024 * 1024):F2} MB");
        }

        container.Dispose();
        Console.WriteLine($"\n--- Final Report ---\nDisk Usage: {(double)GetDirSize(fullPath) / (1024 * 1024):F2} MB");
    }

    private static long GetDirSize(string path)
    {
        if (!Directory.Exists(path)) return 0;
        return Directory.GetFiles(path, "*", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length);
    }
}
