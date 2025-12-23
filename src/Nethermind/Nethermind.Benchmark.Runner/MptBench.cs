// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
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

        for (int i = 0; i < nAccounts; i++)
        {
            byte[] addrBytes = Keccak.Compute(System.Text.Encoding.UTF8.GetBytes($"account-{i}")).Bytes.Slice(0, 20).ToArray();
            Address addr = new Address(addrBytes);
            addrs[i] = addr;

            using (worldState.BeginScope(currentHeader))
            {
                worldState.AddToBalanceAndCreateIfNotExists(addr, (UInt256)1e18, releaseSpec);
                worldState.SetNonce(addr, (UInt256)i);

                for (int j = 0; j < nSlots; j++)
                {
                    UInt256 slotKey = new UInt256(Keccak.Compute(System.Text.Encoding.UTF8.GetBytes($"slot-{j}")).Bytes);
                    byte[] slotVal = Keccak.Compute(System.Text.Encoding.UTF8.GetBytes($"value-{j}")).Bytes.ToArray();
                    worldState.Set(new StorageCell(addr, slotKey), slotVal);
                }

                if ((i + 1) % 10 == 0 || i + 1 == nAccounts)
                {
                    Console.Write($"\r...processed {i + 1}/{nAccounts} accounts ({(double)(i + 1) / nAccounts * 100:F1}%)");
                }

                // Periodic commit
                if ((i + 1) % kCommit == 0 || i + 1 == nAccounts)
                {
                    worldState.Commit(releaseSpec);
                    worldState.CommitTree(i / kCommit);
                    currentRoot = worldState.StateRoot;
                    currentHeader = Build.A.BlockHeader.WithStateRoot(currentRoot).TestObject;
                    
                    // Release memory by resetting world state and suggesting GC
                    worldState.Reset();
                    GC.Collect();
                    
                    long currentSize = GetDirSize(fullPath);
                    Console.WriteLine($"\n[Batch {(i / kCommit) + 1}] Committed. State Root: {currentRoot.ToShortString()}. Disk: {(double)currentSize / (1024 * 1024):F2} MB");
                }
            }
        }

        Console.WriteLine();
        Console.WriteLine($"Creation finished in {sw.Elapsed}. Final Root: {currentRoot}");
        Console.WriteLine($"Disk Usage after Phase 1 (Creation): {(double)GetDirSize(fullPath) / (1024 * 1024):F2} MB");

        // Phase 2: Modification
        mModify = Math.Min(mModify, nAccounts);
        Console.WriteLine($"Phase 2: Randomly modifying slots in {mModify} accounts (k={kCommit})...");
        sw.Restart();

        Random rand = new Random();
        int[] perm = Enumerable.Range(0, nAccounts).OrderBy(x => rand.Next()).ToArray();

        for (int i = 0; i < mModify; i++)
        {
            Address addr = addrs[perm[i]];

            using (worldState.BeginScope(currentHeader))
            {
                // Modify 500 random slots per account
                for (int j = 0; j < 500; j++)
                {
                    int slotIdx = rand.Next(nSlots);
                    UInt256 slotKey = new UInt256(Keccak.Compute(System.Text.Encoding.UTF8.GetBytes($"slot-{slotIdx}")).Bytes);
                    byte[] newVal = Keccak.Compute(System.Text.Encoding.UTF8.GetBytes($"new-value-{i}-{j}")).Bytes.ToArray();
                    worldState.Set(new StorageCell(addr, slotKey), newVal);
                }

                if ((i + 1) % 10 == 0 || i + 1 == mModify)
                {
                    Console.Write($"\r...modified {i + 1}/{mModify} accounts ({(double)(i + 1) / mModify * 100:F1}%)");
                }

                if ((i + 1) % kCommit == 0 || i + 1 == mModify)
                {
                    worldState.Commit(releaseSpec);
                    worldState.CommitTree((i / kCommit) + 1000000);
                    currentRoot = worldState.StateRoot;
                    currentHeader = Build.A.BlockHeader.WithStateRoot(currentRoot).TestObject;
                    
                    worldState.Reset();
                    GC.Collect();
                    long currentSize = GetDirSize(fullPath);
                    Console.WriteLine($"\n[Mod Batch] Committed. State Root: {currentRoot.ToShortString()}. Disk: {(double)currentSize / (1024 * 1024):F2} MB");
                }
            }
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
            var allFiles = Directory.GetFiles(path, "*", SearchOption.AllDirectories)
                .Select(f => new FileInfo(f))
                .ToList();

            long totalSize = allFiles.Sum(f => f.Length);
            
            Console.WriteLine($"\nScanning directory: {path}");
            Console.WriteLine($"Found {allFiles.Count} files. Total size: {totalSize / 1024.0 / 1024.0:F2} MB");

            if (totalSize < 5 * 1024 * 1024) 
            {
                Console.WriteLine("Files found:");
                foreach (var f in allFiles)
                {
                    Console.WriteLine($"  - {Path.GetRelativePath(path, f.FullName)} ({f.Length / 1024.0:F1} KB)");
                }
            }
            else
            {
                Console.WriteLine("Top 5 space consumers:");
                foreach (var f in allFiles.OrderByDescending(f => f.Length).Take(5))
                {
                    Console.WriteLine($"  - {Path.GetRelativePath(path, f.FullName)}: {f.Length / 1024.0 / 1024.0:F2} MB");
                }
            }
            
            return totalSize;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error calculating directory size: {ex.Message}");
            return 0;
        }
    }
}

