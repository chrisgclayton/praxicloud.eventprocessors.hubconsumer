// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.sample
{
    #region Using Clauses
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Consumer;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.metrics;
    using praxicloud.core.metrics.prometheus;
    using praxicloud.eventprocessors.hubconsumer.leasing;
    using praxicloud.eventprocessors.hubconsumer.policies;
    using praxicloud.eventprocessors.hubconsumer.storage;
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    #endregion

    /// <summary>
    /// Entry point type of the console application
    /// </summary>
    class Program
    {
        #region Variables
        /// <summary>
        /// A variable that the processors can increment at the end of their runs to track the total number
        /// </summary>
        public static long TotalMessageCount = 0;

        /// <summary>
        /// A watch used to time the total execution time
        /// </summary>
        private static Stopwatch _watch;

        /// <summary>
        /// True as long as the processors should continue running
        /// </summary>
        private static bool _continueProcessing = true;
        #endregion
        #region Entry Point
        /// <summary>
        /// Entry point of the program
        /// </summary>
        static void Main()
        {
            var task = MainAsync();

            Console.WriteLine("Press <ENTER> to quit.");
            Console.ReadLine();
            _continueProcessing = false;

            try
            {
                task.GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error occurred {e.Message}");
            }
            finally
            {
                Console.WriteLine($"The total number of messages processed was {TotalMessageCount} in {_watch.ElapsedMilliseconds} ms for a total of {(TotalMessageCount / TimeSpan.FromMilliseconds(_watch.ElapsedMilliseconds).TotalSeconds)} / second");
                Console.ReadLine();
            }
        }
        #endregion
        #region Methods
        private static async Task MainAsync()
        {
            var metricFactory = GetMetricFactory();
            var loggerFactory = GetLoggerFactory();
            var logger = loggerFactory.CreateLogger("EphDemo");
            var leaseLogger = loggerFactory.CreateLogger("EphLeaseDemo");
            var checkpointLogger = loggerFactory.CreateLogger("EphCheckpointDemo");
            var checkpointPolicy = new PeriodicCheckpointPolicy(1000, TimeSpan.FromSeconds(15));


            var ConnectionStringPartition = Environment.GetEnvironmentVariable("PraxiDemo:HubConnectionString");
            if (string.IsNullOrWhiteSpace(ConnectionStringPartition)) throw new ApplicationException("The Event Hub Connection string must be in the environment variable named 'PraxiDemo:HubConnectionString'");

            var ConnectionStringStorage = Environment.GetEnvironmentVariable("PraxiDemo:StorageConnectionString");
            if (string.IsNullOrWhiteSpace(ConnectionStringStorage)) throw new ApplicationException("The Azure Storage Connection string must be in the environment variable named 'PraxiDemo:StorageConnectionString'");

            var leaseManager = new FixedLeaseManager(leaseLogger, metricFactory, ConnectionStringPartition, 0, 1);
            var checkpointManager = new BlobStorageMetadataCheckpointManager(checkpointLogger, metricFactory, "checkpoints", ConnectionStringStorage);
            var processorOptions = new FixedProcessorClientOptions
            {
                CheckpointPrefix = null,
                BatchSize = 50,
                ConnectionOptions = new EventHubConnectionOptions() { TransportType = EventHubsTransportType.AmqpTcp },
                Identifier = "demoprocessor1",
                LeaseDuration = TimeSpan.FromSeconds(20),
                LeaseRenewalInterval = TimeSpan.FromSeconds(10),
                PrefetchCount = 300,
                ReceiveTimeout = TimeSpan.FromSeconds(120),
                RetryOptions = new EventHubsRetryOptions
                {
                    Delay = TimeSpan.FromSeconds(2),
                    MaximumDelay = TimeSpan.FromSeconds(10),
                    MaximumRetries = 3,
                    Mode = EventHubsRetryMode.Exponential,
                    TryTimeout = TimeSpan.FromSeconds(2)
                },
                StartingPosition = Azure.Messaging.EventHubs.Consumer.EventPosition.Earliest,
                TrackLastEnqueuedEventProperties = true
            };

            var poisonMessageMonitor = new BlobStorageCountPoisonedMessageMonitor(logger, metricFactory, "poisonmonitor", ConnectionStringStorage, 4);

            var processor = new FixedBatchProcessorClient<BatchProcessor>(logger, metricFactory, ConnectionStringPartition, "$default", processorOptions, leaseManager, checkpointManager, (logger, metricFactory, partitionContext) =>
            {
                return new BatchProcessor(checkpointPolicy, poisonMessageMonitor);
            });

            await leaseManager.InitializeAsync((FixedProcessorClient)processor, processorOptions, CancellationToken.None).ConfigureAwait(false);
            await checkpointManager.InitializeAsync(processor, processorOptions, CancellationToken.None).ConfigureAwait(false);
            await poisonMessageMonitor.InitializeAsync(processor, processorOptions, logger, metricFactory, CancellationToken.None).ConfigureAwait(false);
            await poisonMessageMonitor.CreateStoreIfNotExistsAsync(processor, processorOptions, CancellationToken.None).ConfigureAwait(false);

            await processor.InitializeAsync(CancellationToken.None).ConfigureAwait(false);
            await processor.StartProcessingAsync(CancellationToken.None).ConfigureAwait(false);

            _watch = Stopwatch.StartNew();

            while (_continueProcessing)
            {
                await Task.Delay(500).ConfigureAwait(false);
            }

            await processor.StopProcessingAsync(CancellationToken.None).ConfigureAwait(false);
            _watch.Stop();
        }

        private static ILoggerFactory GetLoggerFactory()
        {
            var builder = new ServiceCollection();

            builder.AddLogging(configuration =>
            {
                configuration.AddConsole(options =>
                {
                    options.DisableColors = false;
                    options.IncludeScopes = false;
                });

                configuration.SetMinimumLevel(LogLevel.Debug);
            });

            var provider = builder.BuildServiceProvider();

            return provider.GetRequiredService<ILoggerFactory>();
        }


        private static IMetricFactory GetMetricFactory()
        {
            var factory = new MetricFactory();

            factory.AddPrometheus("prom", 9600);

            return factory;
        }
        #endregion
    }
}
