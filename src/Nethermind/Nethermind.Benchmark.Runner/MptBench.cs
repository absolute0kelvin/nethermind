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
        
        // 切换回 Nethermind 优化的 Path 方案
        initConfig.StateDbKeyScheme = INodeStorage.KeyScheme.Path;

        IPruningConfig pruningConfig = configProvider.GetConfig<IPruningConfig>();
        // 调整为生产最常用的 Hybrid 模式
        pruningConfig.Mode = PruningMode.Hybrid; 
        pruningConfig.PersistenceInterval = 1;
        pruningConfig.DirtyCacheMb = 256; 
        pruningConfig.PruningBoundary = 64; // 保留最近 64 个块的状态

        IDbConfig dbConfig = configProvider.GetConfig<IDbConfig>();
        dbConfig.WriteAheadLogSync = false; 
        
        // 显式关闭所有压缩
        dbConfig.RocksDbOptions = "compression=kNoCompression;bottommost_compression=kNoCompression;level0_file_num_compaction_trigger=4;";

        var container = new ContainerBuilder()
            .AddModule(new TestNethermindModule(configProvider))
            .AddSingleton<IRocksDbConfigFactory, RocksDbConfigFactory>()
            .AddSingleton<IDbFactory, RocksDbFactory>()
            .Build();

        IWorldStateManager worldStateManager = container.Resolve<IWorldStateManager>();
        IWorldState worldState = worldStateManager.GlobalWorldState;
        IReleaseSpec releaseSpec = new Prague();

        Console.WriteLine($"Phase 1: Creating {nAccounts} accounts with variable slots (avg {nSlots}, Scheme: Path)...");
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

                    // 优化：不再是固定 1000 个，而是在 0 到 nSlots*2 之间波动
                    int variableSlots = rand.Next(nSlots * 2);
                    for (int j = 0; j < variableSlots; j++)
                    {
                        UInt256 slotKey = new UInt256(Keccak.Compute(System.Text.Encoding.UTF8.GetBytes($"acc-{i}-slot-{j}")).Bytes);
                        
                        // 优化：模拟真实数据，30% 的概率写入零值或小值，提高压缩测试的真实性
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

                    // 每次修改随机 100 个 Slots
                    for (int j = 0; j < 100; j++)
                    {
                        int slotIdx = rand.Next(nSlots); // 尝试修改可能存在的 Slot
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
