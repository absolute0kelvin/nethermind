// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.IO;
using System.Linq;
using Autofac;
using BenchmarkDotNet.Attributes;
using DotNetty.Common.Utilities;
using Nethermind.Api;
using Nethermind.Config;
using Nethermind.Core;
using Nethermind.Core.Specs;
using Nethermind.Core.Test.Builders;
using Nethermind.Core.Test.Modules;
using Nethermind.Db;
using Nethermind.Db.Rocks.Config;
using Nethermind.Evm.State;
using Nethermind.Int256;
using Nethermind.Specs.Forks;
using Nethermind.State;

namespace Nethermind.Benchmarks.Store;

public class WorldStateBenchmarks
{
    private IContainer _container;
    private IWorldState _globalWorldState;
    private string _tempDbPath;

    [Params(4096, 16384)]
    public int AccountCount { get; set; }

    [Params(128, 512)]
    public int ContractCount { get; set; }

    public int SlotsCount => ContractCount * 128;

    [Params(4096, 16384)]
    public int BigContractSlotsCount { get; set; }

    [Params(1024, 10240)]
    public int LoopSize { get; set; }

    private Address[] _accounts;
    private Address[] _contracts;
    private (Address Account, UInt256 Slot)[] _slots;
    private Address _bigContract;
    private UInt256[] _bigContractSlots;
    private IReleaseSpec _releaseSpec = new Prague();
    private BlockHeader _baseBlock;

    [GlobalSetup]
    public void Setup()
    {
        _tempDbPath = Path.Combine(Path.GetTempPath(), "nethermind_benchmark_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDbPath);

        ConfigProvider configProvider = new();
        IInitConfig initConfig = configProvider.GetConfig<IInitConfig>();
        initConfig.BaseDbPath = _tempDbPath;
        initConfig.DiagnosticMode = DiagnosticMode.None; // Ensure it uses RocksDb

        IPruningConfig pruningConfig = configProvider.GetConfig<IPruningConfig>();
        pruningConfig.Mode = PruningMode.None; // Archive mode persists everything
        pruningConfig.PersistenceInterval = 1;

        IDbConfig dbConfig = configProvider.GetConfig<IDbConfig>();
        // You can also tune RocksDb options here if needed
        // dbConfig.StateDbRocksDbOptions = "...";

        _container = new ContainerBuilder()
            .AddModule(new TestNethermindModule(configProvider))
            .Build();

        IWorldState worldState = _globalWorldState = _container.Resolve<IWorldStateManager>().GlobalWorldState;
        using var _ = worldState.BeginScope(IWorldState.PreGenesis);

        Random rand = new Random(0);
        byte[] randomBuffer = new byte[20];
        _accounts = new Address[AccountCount];
        for (int i = 0; i < AccountCount; i++)
        {
            rand.NextBytes(randomBuffer);
            Address account = new Address(randomBuffer.ToArray());
            worldState.AddToBalanceAndCreateIfNotExists(account, (UInt256)rand.NextLong() + 1, _releaseSpec);
            _accounts[i] = account;
        }

        _contracts = new Address[ContractCount];
        for (int i = 0; i < ContractCount; i++)
        {
            rand.NextBytes(randomBuffer);
            Address account = new Address(randomBuffer.ToArray());
            worldState.AddToBalanceAndCreateIfNotExists(account, (UInt256)rand.NextLong() + 1, _releaseSpec);
            _contracts[i] = account;
        }

        _slots = new (Address, UInt256)[SlotsCount];
        for (int i = 0; i < SlotsCount; i++)
        {
            Address account = _contracts[rand.Next(0, _contracts.Length)];
            UInt256 slot = (UInt256)rand.NextLong();
            rand.NextBytes(randomBuffer);
            worldState.Set(new StorageCell(account, slot), randomBuffer.ToArray());
            _slots[i] = (account, slot);
        }

        rand.NextBytes(randomBuffer);
        _bigContract = new Address(randomBuffer.ToArray());
        worldState.AddToBalanceAndCreateIfNotExists(_bigContract, 1, _releaseSpec);
        _bigContractSlots = new UInt256[BigContractSlotsCount];
        for (int i = 0; i < BigContractSlotsCount; i++)
        {
            UInt256 slot = (UInt256)rand.NextLong();
            rand.NextBytes(randomBuffer);
            _bigContractSlots[i] = slot;
            worldState.Set(new StorageCell(_bigContract, slot), randomBuffer.ToArray());
        }

        worldState.Commit(_releaseSpec);
        worldState.CommitTree(0);
        worldState.Reset();
        _baseBlock = Build.A.BlockHeader.WithStateRoot(worldState.StateRoot).TestObject;
    }

    [GlobalCleanup]
    public void Teardown()
    {
        _container.Dispose();
        if (Directory.Exists(_tempDbPath))
        {
            try
            {
                Directory.Delete(_tempDbPath, true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }

    [Benchmark]
    public void AccountRead()
    {
        Random rand = new Random(1);
        IWorldState worldState = _globalWorldState;
        using var _ = worldState.BeginScope(_baseBlock);

        for (int i = 0; i < LoopSize; i++)
        {
            worldState.GetBalance(_accounts[rand.Next(0, _accounts.Length)]);
        }

        worldState.Reset();
    }

    [Benchmark]
    public void AccountReadWrite()
    {
        Random rand = new Random(1);
        IWorldState worldState = _globalWorldState;
        using var _ = worldState.BeginScope(_baseBlock);

        for (int i = 0; i < LoopSize; i++)
        {
            if (rand.NextDouble() < 0.5)
            {
                worldState.GetBalance(_accounts[rand.Next(0, _accounts.Length)]);
            }
            else
            {
                worldState.AddToBalance(_accounts[rand.Next(0, _accounts.Length)], 1, _releaseSpec);
            }
        }

        worldState.Commit(_releaseSpec);
        worldState.CommitTree(1);
        worldState.Reset();
    }

    [Benchmark]
    public void SlotRead()
    {
        Random rand = new Random(1);
        IWorldState worldState = _globalWorldState;
        using var _ = worldState.BeginScope(_baseBlock);

        for (int i = 0; i < LoopSize; i++)
        {
            (Address Account, UInt256 Slot) slot = _slots[rand.Next(0, _slots.Length)];
            worldState.Get(new StorageCell(slot.Account, slot.Slot));
        }

        worldState.Reset();
    }

    [Benchmark]
    public void SlotReadWrite()
    {
        Random rand = new Random(1);
        IWorldState worldState = _globalWorldState;
        using var _ = worldState.BeginScope(_baseBlock);
        byte[] randomBuffer = new byte[20];

        for (int i = 0; i < LoopSize; i++)
        {
            (Address Account, UInt256 Slot) slot = _slots[rand.Next(0, _slots.Length)];
            if (rand.NextDouble() < 0.5)
            {
                worldState.Get(new StorageCell(slot.Account, slot.Slot));
            }
            else
            {
                rand.NextBytes(randomBuffer);
                worldState.Set(new StorageCell(slot.Account, slot.Slot), randomBuffer.ToArray());
            }
        }

        worldState.Commit(_releaseSpec);
        worldState.CommitTree(1);
        worldState.Reset();
    }

    [Benchmark]
    public void SameContractRead()
    {
        Random rand = new Random(1);
        IWorldState worldState = _globalWorldState;
        using var _ = worldState.BeginScope(_baseBlock);

        for (int i = 0; i < LoopSize; i++)
        {
            UInt256 slot = _bigContractSlots[rand.Next(0, _bigContractSlots.Length)];
            worldState.Get(new StorageCell(_bigContract, slot));
        }

        worldState.Reset();
    }

    [Benchmark]
    public void SameContractReadWrite()
    {
        Random rand = new Random(1);
        IWorldState worldState = _globalWorldState;
        using var _ = worldState.BeginScope(_baseBlock);
        byte[] randomBuffer = new byte[20];

        for (int i = 0; i < LoopSize; i++)
        {
            UInt256 slot = _bigContractSlots[rand.Next(0, _bigContractSlots.Length)];
            if (rand.NextDouble() < 0.5)
            {
                worldState.Get(new StorageCell(_bigContract, slot));
            }
            else
            {
                rand.NextBytes(randomBuffer);
                worldState.Set(new StorageCell(_bigContract, slot), randomBuffer.ToArray());
            }
        }

        worldState.Commit(_releaseSpec);
        worldState.CommitTree(1);
        worldState.Reset();
    }
}
