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
        initConfig.DiagnosticMode = DiagnosticMode.None;

        // 显式设置 DataDir，防止它跑去默认的 logs 或其他地方
        initConfig.DataDir = fullPath;

        IPruningConfig pruningConfig = configProvider.GetConfig<IPruningConfig>();
        pruningConfig.Mode = PruningMode.None; // Archive mode
        pruningConfig.PersistenceInterval = 1;
        pruningConfig.DirtyCacheMb = 1; // 强制极小缓存，迫使数据下刷到磁盘
        pruningConfig.PruningBoundary = 0; // 强制立即持久化

        IDbConfig dbConfig = configProvider.GetConfig<IDbConfig>();
        dbConfig.WriteAheadLogSync = true;

        var container = new ContainerBuilder()
            .AddModule(new TestNethermindModule(configProvider))
            .AddSingleton<IRocksDbConfigFactory, RocksDbConfigFactory>()
            .AddSingleton<IDbFactory, RocksDbFactory>()
            .Build();

        IWorldStateManager worldStateManager = container.Resolve<IWorldStateManager>();
        IWorldState worldState = worldStateManager.GlobalWorldState;
        
        IReleaseSpec releaseSpec = new Prague();

        // Phase 1: Creation
        Console.WriteLine($"Phase 1: Creating {nAccounts} accounts with {nSlots} slots each (k={kCommit})...");
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

                    for (int j = 0; j < nSlots; j++)
                    {
                        // 确保每个账号的 Key 都是唯一的
                        UInt256 slotKey = new UInt256(Keccak.Compute(System.Text.Encoding.UTF8.GetBytes($"acc-{i}-slot-{j}")).Bytes);
                        rand.NextBytes(valBuffer);
                        worldState.Set(new StorageCell(addr, slotKey), valBuffer.ToArray());
                    }

                    if ((i + 1) % 10 == 0 || i + 1 == nAccounts)
                    {
                        Console.Write($"\r...processed {i + 1}/{nAccounts} accounts ({(double)(i + 1) / nAccounts * 100:F1}%)");
                    }
                }

                // Batch commit
                worldState.Commit(releaseSpec);
                worldState.CommitTree(batchStart / kCommit);
                currentRoot = worldState.StateRoot;
                currentHeader = Build.A.BlockHeader.WithStateRoot(currentRoot).TestObject;
                
                worldState.Reset();
            }

            worldStateManager.FlushCache(CancellationToken.None);
            
            GC.Collect(2, GCCollectionMode.Forced, true);
            long currentSize = GetDirSize(fullPath);
            Console.WriteLine($"\r[Batch {(batchStart / kCommit) + 1}/{Math.Ceiling((double)nAccounts / kCommit)}] Root: {currentRoot.ToShortString()} | Disk: {currentSize / 1024.0 / 1024.0:F2} MB | Memory: {Process.GetCurrentProcess().WorkingSet64 / 1024.0 / 1024.0:F2} MB");
        }

        Console.WriteLine();
        Console.WriteLine($"Creation finished in {sw.Elapsed}. Final Root: {currentRoot}");
        Console.WriteLine($"Disk Usage after Phase 1 (Creation): {(double)GetDirSize(fullPath) / (1024 * 1024):F2} MB");

        // Phase 2: Modification
        mModify = Math.Min(mModify, nAccounts);
        Console.WriteLine($"Phase 2: Randomly modifying slots in {mModify} accounts (k={kCommit})...");
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

                    // Modify 500 random slots per account
                    for (int j = 0; j < 500; j++)
                    {
                        int slotIdx = rand.Next(nSlots);
                        // 重新计算该账号对应的原始唯一 Key
                        UInt256 slotKey = new UInt256(Keccak.Compute(System.Text.Encoding.UTF8.GetBytes($"acc-{accountIdx}-slot-{slotIdx}")).Bytes);
                        rand.NextBytes(valBuffer);
                        worldState.Set(new StorageCell(addr, slotKey), valBuffer.ToArray());
                    }

                    if ((i + 1) % 10 == 0 || i + 1 == mModify)
                    {
                        Console.Write($"\r...modified {i + 1}/{mModify} accounts ({(double)(i + 1) / mModify * 100:F1}%)");
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
            long currentSize = GetDirSize(fullPath);
            Console.WriteLine($"\n[Mod Batch] Committed. State Root: {currentRoot.ToShortString()}. Disk: {(double)currentSize / (1024 * 1024):F2} MB");
        }

        Console.WriteLine();
        Console.WriteLine($"Modification finished in {sw.Elapsed}. Final New Root: {currentRoot}");

        container.Dispose();

        // Final Report
        long totalSize = GetDirSize(fullPath);
        Console.WriteLine("\n--- Final Report ---");
        Console.WriteLine($"Database Path: {fullPath}");
        Console.WriteLine($"Disk Usage:    {(double)totalSize / (1024 * 1024):F2} MB");
    }

    private static long GetDirSize(string path)
    {
        if (!Directory.Exists(path)) return 0;
        try
        {
            return Directory.GetFiles(path, "*", SearchOption.AllDirectories)
                .Select(f => new FileInfo(f))
                .Sum(f => f.Length);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error calculating directory size: {ex.Message}");
            return 0;
        }
    }
}
