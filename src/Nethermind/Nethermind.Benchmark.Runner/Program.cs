// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using System.Linq;
using BenchmarkDotNet.Toolchains.InProcess.NoEmit;
using BenchmarkDotNet.Columns;
using Nethermind.Benchmarks.State;
using Nethermind.Precompiles.Benchmark;

namespace Nethermind.Benchmark.Runner
{
    public class DashboardConfig : ManualConfig
    {
        public DashboardConfig(params Job[] jobs)
        {
            foreach (Job job in jobs)
            {
                AddJob(job.WithToolchain(InProcessNoEmitToolchain.Instance));
            }

            AddColumnProvider(DefaultColumnProviders.Descriptor);
            AddColumnProvider(DefaultColumnProviders.Statistics);
            AddColumnProvider(DefaultColumnProviders.Params);
            AddColumnProvider(DefaultColumnProviders.Metrics);
            AddLogger(BenchmarkDotNet.Loggers.ConsoleLogger.Default);
            AddExporter(BenchmarkDotNet.Exporters.Json.JsonExporter.FullCompressed);
            AddDiagnoser(BenchmarkDotNet.Diagnosers.MemoryDiagnoser.Default);
            WithSummaryStyle(SummaryStyle.Default.WithMaxParameterColumnWidth(100));
        }
    }

    public class PrecompileBenchmarkConfig : DashboardConfig
    {
        public PrecompileBenchmarkConfig() : base(Job.MediumRun)
        {
            AddColumnProvider(new GasColumnProvider());
        }
    }

    public static class Program
    {
        public static void Main(string[] args)
        {
            if (args.Length > 0 && args[0] == "mpt-bench")
            {
                int n = 100;
                int slots = 1000;
                int mModify = 10;
                int kCommit = 50;
                if (args.Length > 1) int.TryParse(args[1], out n);
                if (args.Length > 2) int.TryParse(args[2], out slots);
                if (args.Length > 3) int.TryParse(args[3], out mModify);
                if (args.Length > 4) int.TryParse(args[4], out kCommit);
                MptBench.Run(n, slots, mModify, kCommit);
                return;
            }

            List<Assembly> additionalJobAssemblies = [
                typeof(JsonRpc.Benchmark.EthModuleBenchmarks).Assembly,
                typeof(Benchmarks.Core.Keccak256Benchmarks).Assembly,
                typeof(Evm.Benchmark.EvmStackBenchmarks).Assembly,
                typeof(Network.Benchmarks.DiscoveryBenchmarks).Assembly,
            ];

            List<Assembly> simpleJobAssemblies = [
                // typeof(EthereumTests.Benchmark.EthereumTests).Assembly,
            ];

            if (Debugger.IsAttached)
            {
                BenchmarkSwitcher.FromAssemblies(additionalJobAssemblies.Union(simpleJobAssemblies).ToArray()).RunAll(new DebugInProcessConfig());
            }
            else
            {
                foreach (Assembly assembly in additionalJobAssemblies)
                {
                    BenchmarkRunner.Run(assembly, new DashboardConfig(Job.MediumRun), args);
                }

                foreach (Assembly assembly in simpleJobAssemblies)
                {
                    BenchmarkRunner.Run(assembly, new DashboardConfig(), args);
                }

                BenchmarkRunner.Run(typeof(KeccakBenchmark).Assembly, new PrecompileBenchmarkConfig(), args);
            }
        }
    }
}
