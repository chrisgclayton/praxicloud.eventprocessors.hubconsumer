// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.sample
{
    #region Using Clauses
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Processor;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.metrics;
    using praxicloud.eventprocessors.hubconsumer.policies;
    using praxicloud.eventprocessors.hubconsumer.processors;
    #endregion

    /// <summary>
    /// A batch event processor
    /// </summary>
    public sealed class BatchProcessor : Processor, IEventBatchProcessor
    {
        #region Variables
        /// <summary>
        /// The default event array used in callbacks
        /// </summary>
        private static readonly EventData[] _defaultEventArray = new EventData[0];

        /// <summary>
        /// The maximum interval to delay before checkpointing if the batch is not full
        /// </summary>
        private static long CheckpointInterval = 10000;

        /// <summary>
        /// The last event data received for checkpointing use
        /// </summary>
        private EventData _lastData = default;

        /// <summary>
        /// The number of messages that have beenr eceived
        /// </summary>
        private long _messageCount;

        /// <summary>
        /// The next count to checkpoint at
        /// </summary>
        private long _nextCheckpoint;

        /// <summary>
        /// The checkpointing policy in use
        /// </summary>
        private readonly ICheckpointPolicy _policy;

        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        public BatchProcessor()
        {
            _policy = new PeriodicCheckpointPolicy(1000, TimeSpan.FromSeconds(15));
        }
        #endregion
        #region Methods
        /// <inheritdoc />
        public override async Task InitializeAsync(ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext partitionContext)
        {
            await base.InitializeAsync(logger, metricFactory, partitionContext).ConfigureAwait(false);
            await _policy.InitializeAsync(Logger, metricFactory, partitionContext, CancellationToken.None).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public override async Task PartitionInitializeAsync(ProcessorPartitionContext partitionContext, CancellationToken cancellationToken)
        {
            await base.PartitionInitializeAsync(partitionContext, cancellationToken).ConfigureAwait(false);
            _nextCheckpoint = CheckpointInterval;
        }

        /// <inheritdoc />
        public override async Task PartitionStopAsync(ProcessorPartitionContext partitionContext, ProcessingStoppedReason reason, CancellationToken cancellationToken)
        {
            await base.PartitionStopAsync(partitionContext, reason, cancellationToken).ConfigureAwait(false);

            using (Logger.BeginScope("Batch OnStop"))
            {
                if (reason == ProcessingStoppedReason.Shutdown && _lastData != default)
                {
                    Logger.LogDebug("Checkpointing {result} for partition {partitionId} during graceful shutdown", await _policy.CheckpointAsync(_lastData, true, cancellationToken).ConfigureAwait(false), partitionContext);
                }
            }

            Interlocked.Add(ref Program.TotalMessageCount, _messageCount);
        }

        /// <inheritdoc />
        public async Task PartitionProcessAsync(IEnumerable<EventData> events, ProcessorPartitionContext partitionContext, CancellationToken cancellationToken)
        {
            using (Logger.BeginScope("Processing Event Batch"))
            {
                var eventList = events?.ToArray() ?? _defaultEventArray;

                if (eventList.Length > 0)
                {
                    Logger.LogInformation("Events received for partition {partitionId}: {count}", partitionContext.PartitionId, eventList.Length);
                    MessageCounter.IncrementBy(eventList.Length);
                    _messageCount += eventList.Length;
                    _lastData = eventList[eventList.Length - 1];

                    _policy.IncrementBy(eventList.Length);
                }
                else
                {
                    Logger.LogInformation("No events in batch for partition {partitionId}", partitionContext.PartitionId);
                }

                if (_lastData != default)
                {
                    Logger.LogDebug("Checkpointing {result} for partition {partitionId}", await _policy.CheckpointAsync(_lastData, false, cancellationToken).ConfigureAwait(false), partitionContext);
                }
            }
        }
        #endregion
    }
}
