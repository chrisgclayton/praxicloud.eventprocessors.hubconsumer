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
    #endregion

    /// <summary>
    /// A batch event processor
    /// </summary>
    public sealed class BatchProcessor : IEventBatchProcessor
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
        /// A logger to write debugging and diagnostics information to
        /// </summary>
        private ILogger _logger;

        /// <summary>
        /// A metric container that counts the number of messages received
        /// </summary>
        private ICounter _messageCounter;

        /// <summary>
        /// A metric counter that counts the number of errors that have occured
        /// </summary>
        private ICounter _errorCounter;

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
        public async Task InitializeAsync(ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext partitionContext)
        {
            _logger = logger;

            using (_logger.BeginScope("Initialize batch processor"))
            {
                _logger.LogInformation("Initializing batch processor for partition {partitionId}", partitionContext.PartitionId);
                _messageCounter = metricFactory.CreateCounter($"ebp-message-count-{partitionContext.PartitionId}", "The number of messages that have been processed by partition {partitionContext.PartitionId}", false, new string[0]);
                _errorCounter = metricFactory.CreateCounter($"ebp-error-count-{partitionContext.PartitionId}", "The number of errors that have been processed by partition {partitionContext.PartitionId}", false, new string[0]);

                await _policy.InitializeAsync(_logger, metricFactory, partitionContext, CancellationToken.None).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public Task PartitionInitializeAsync(ProcessorPartitionContext partitionContext, CancellationToken cancellationToken)
        {
            using (_logger.BeginScope("OnInitialize"))
            {
                _nextCheckpoint = CheckpointInterval;

                _logger.LogInformation("Initializing partition {partitionId}", partitionContext.PartitionId);
            }

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public async Task PartitionStopAsync(ProcessorPartitionContext partitionContext, ProcessingStoppedReason reason, CancellationToken cancellationToken)
        {
            using (_logger.BeginScope("OnStop"))
            {
                _logger.LogInformation("Stopping partition {partitionId}, reason {reason}", partitionContext.PartitionId, Enum.GetName(typeof(ProcessingStoppedReason), reason));

                if (reason == ProcessingStoppedReason.Shutdown && _lastData != default)
                {
                    _logger.LogInformation("Checkpointing in graceful shutdown for partition {partitionId}, sequence {sequenceNumber}", partitionContext.PartitionId, Enum.GetName(typeof(ProcessingStoppedReason), reason), _lastData.SequenceNumber);
                    _logger.LogDebug("Checkpointing {result} for partition {partitionId} during graceful shutdown", await _policy.CheckpointAsync(_lastData, true, cancellationToken).ConfigureAwait(false), partitionContext);
                    _logger.LogInformation("Checkpointed in graceful shutdown for partition {partitionId}, sequence {sequenceNumber}", partitionContext.PartitionId, Enum.GetName(typeof(ProcessingStoppedReason), reason), _lastData.SequenceNumber);
                }
            }

            Interlocked.Add(ref Program.TotalMessageCount, _messageCount);
        }

        /// <inheritdoc />
        public Task PartitionHandleErrorAsync(Exception exception, ProcessorPartitionContext partitionContext, string operationDescription, CancellationToken cancellationToken)
        {
            using (_logger.BeginScope("OnError"))
            {
                _errorCounter.Increment();
                _logger.LogError(exception, "Erron partition {partitionId}, {operationDescription}", partitionContext.PartitionId, operationDescription);
            }

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public async Task PartitionProcessAsync(IEnumerable<EventData> events, ProcessorPartitionContext partitionContext, CancellationToken cancellationToken)
        {
            using (_logger.BeginScope("Processing Event Batch"))
            {
                var eventList = events?.ToArray() ?? _defaultEventArray;

                if (eventList.Length > 0)
                {
                    _logger.LogInformation("Events received for partition {partitionId}: {count}", partitionContext.PartitionId, eventList.Length);
                    _messageCounter.IncrementBy(eventList.Length);
                    _messageCount += eventList.Length;
                    _lastData = eventList[eventList.Length - 1];

                    _policy.IncrementBy(eventList.Length);
                }
                else
                {
                    _logger.LogInformation("No events in batch for partition {partitionId}", partitionContext.PartitionId);
                }

                if (_lastData != default)
                {
                    _logger.LogDebug("Checkpointing {result} for partition {partitionId}", await _policy.CheckpointAsync(_lastData, false, cancellationToken).ConfigureAwait(false), partitionContext);
                }
            }
        }
        #endregion
    }
}
