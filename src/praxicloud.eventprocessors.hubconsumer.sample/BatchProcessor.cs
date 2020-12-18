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
    using Microsoft.Extensions.Options;
    using praxicloud.core.metrics;
    using praxicloud.core.security;
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
        /// The last event data received for checkpointing use
        /// </summary>
        private EventData _lastData = default;

        /// <summary>
        /// The number of messages that have beenr eceived
        /// </summary>
        private long _messageCount;

        /// <summary>
        /// The checkpointing policy in use
        /// </summary>
        private readonly ICheckpointPolicy _policy;

        /// <summary>
        /// The poison message monitor in use by the batch
        /// </summary>
        private readonly IPoisonedMonitor _poisonedMonitor;

        /// <summary>
        /// True if the batch is the first to be processed
        /// </summary>
        private bool _isFirstBatch = true;
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        public BatchProcessor(ICheckpointPolicy checkpointPolicy, IPoisonedMonitor poisonMonitor)
        {
            Guard.NotNull(nameof(checkpointPolicy), checkpointPolicy);
            Guard.NotNull(nameof(poisonMonitor), poisonMonitor);

            _poisonedMonitor = poisonMonitor;
            _policy = checkpointPolicy; 
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
        public override async Task PartitionStopAsync(ProcessorPartitionContext partitionContext, ProcessingStoppedReason reason, CancellationToken cancellationToken)
        {
            await base.PartitionStopAsync(partitionContext, reason, cancellationToken).ConfigureAwait(false);

            using (Logger.BeginScope("Batch OnStop"))
            {
                if (reason == ProcessingStoppedReason.Shutdown && _lastData != default)
                {
                    var checkpointSuccess = false;

                    try
                    {
                        Logger.LogDebug("Checkpointing for partition {partitionId} during graceful shutdown", partitionContext.PartitionId);
                        checkpointSuccess = await _policy.CheckpointAsync(_lastData, true, cancellationToken).ConfigureAwait(false);
                        Logger.LogInformation("Checkpointing for partition {partitionId} returned {checkpointSuccess}", partitionContext.PartitionId, checkpointSuccess);
                    }
                    catch (Exception e)
                    {
                        Logger.LogError(e, "Error checkpointing during graceful shutdown");
                    }
                }
            }

            Interlocked.Add(ref Program.TotalMessageCount, _messageCount);
        }

        private async Task<bool> UpdatePoisonMonitorStatusAsync(EventData data, ProcessorPartitionContext partitionContext, CancellationToken cancellationToken)
        {
            var shouldProcess = false;

            try
            {
                Logger.LogInformation("Partition {partitionId} checking if Sequence Number {sequenceNumber} is poisoned", partitionContext.PartitionId, data.SequenceNumber);

                if (await _poisonedMonitor.IsPoisonedMessageAsync(partitionContext.PartitionId, data.SequenceNumber, cancellationToken).ConfigureAwait(false))
                {
                    var handled = false;

                    for (var handleIndex = 0; handleIndex < 3 && !handled; handleIndex++)
                    {
                        try
                        {
                            Logger.LogWarning("Partition {partitionId} found Sequence Number {sequenceNumber} is poisoned", partitionContext.PartitionId, data.SequenceNumber);
                            handled = await HandlePoisonMessageAsync(data, partitionContext, cancellationToken).ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            Logger.LogError(e, "Partition {partitionId} error handling poison message {sequenceNumber}", partitionContext.PartitionId, data.SequenceNumber);
                        }
                    }

                    var checkpointSuccess = false;

                    for (var checkpointAttempt = 0; checkpointAttempt < 3 && !checkpointSuccess; checkpointAttempt++)
                    {
                        // Immediately checkpoint to move this forward and do not process further
                        await _policy.CheckpointAsync(data, true, cancellationToken).ConfigureAwait(false);
                        checkpointSuccess = true;
                    }
                }
                else
                {
                    Logger.LogInformation("Partition {partitionId} Sequence Number {sequenceNumber} is not poisoned", partitionContext.PartitionId, data.SequenceNumber);

                    shouldProcess = true;
                    PoisonData poisonData = null;

                    for (var retrieveAttempt = 0; retrieveAttempt < 3 && poisonData == null; retrieveAttempt++)
                    {
                        try
                        {
                            poisonData = await _poisonedMonitor.GetPoisonDataAsync(partitionContext.PartitionId, cancellationToken).ConfigureAwait(false);
                        }
                        catch(Exception e)
                        {
                            Logger.LogError(e, "Error retrieving poison message data for partition {partitionId}", partitionContext.PartitionId);
                        }
                    }

                    if (poisonData.SequenceNumber == -1)
                    {
                        poisonData.SequenceNumber = data.SequenceNumber;
                        poisonData.ReceiveCount = 0;
                    }
                    else if(poisonData.SequenceNumber == data.SequenceNumber)
                    {
                        poisonData.ReceiveCount = poisonData.ReceiveCount + 1;
                    }
                    else
                    {
                        poisonData.ReceiveCount = 1;
                        poisonData.SequenceNumber = data.SequenceNumber;
                    }

                    var updateSuccess = false;

                    for (var retrieveAttempt = 0; retrieveAttempt < 3 && !updateSuccess; retrieveAttempt++)
                    {
                        try
                        {
                            updateSuccess = await _poisonedMonitor.UpdatePoisonDataAsync(poisonData, cancellationToken).ConfigureAwait(false);
                        }
                        catch(Exception e)
                        {
                            Logger.LogError(e, "Error updating poison message data for partition {partitionId}", partitionContext.PartitionId);
                        }
                    }                    
                }                
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Unknown error cannot in poison message handling for partition {partitionId} with sequence Number {sequenceNumber}, ignoring message", partitionContext.PartitionId, data.SequenceNumber);
            }

            return shouldProcess;
        }

        private Task<bool> HandlePoisonMessageAsync(EventData data, ProcessorPartitionContext partitionContext, CancellationToken cancellationToken)
        {
            Logger.LogWarning("Partition {partitionId}, consider poison message Sequence Number {sequenceNumber} handled", partitionContext.PartitionId, data.SequenceNumber);

            return Task.FromResult(true);
        }

        private async Task DelayForABitAsync()
        {
            await Task.Delay(5000).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task PartitionProcessAsync(IEnumerable<EventData> events, ProcessorPartitionContext partitionContext, CancellationToken cancellationToken)
        {
            using (Logger.BeginScope("Processing Event Batch"))
            {
                if(events != null)
                {
                    var eventList = events.ToList();

                    if (eventList.Count > 0)
                    {
                        if (_isFirstBatch)
                        {
                            if (!await UpdatePoisonMonitorStatusAsync(eventList[0], partitionContext, cancellationToken).ConfigureAwait(false))
                            {
                                eventList.RemoveAt(0);
                            }

                            _isFirstBatch = false;
                        }

                        if(eventList.Count > 0)
                        {
                            Logger.LogInformation("Events received for partition {partitionId}: {count}", partitionContext.PartitionId, eventList.Count);
                            MessageCounter.IncrementBy(eventList.Count);
                            _messageCount += eventList.Count;
                            _lastData = eventList[eventList.Count - 1];

                            _policy.IncrementBy(eventList.Count);

                            await DelayForABitAsync().ConfigureAwait(false);
                            var checkpointResult = await _policy.CheckpointAsync(_lastData, false, cancellationToken).ConfigureAwait(false);
                        }
                        else
                        {
                            Logger.LogInformation("No events in batch for partition {partitionId}, removed in poison message test", partitionContext.PartitionId);
                        }
                    }
                }
                else
                {
                    if (_lastData != default)
                    {
                        Logger.LogDebug("No events in batch checkpointing for partition {partitionId}", partitionContext.PartitionId);

                        try
                        {
                            var checkpointResult = await _policy.CheckpointAsync(_lastData, false, cancellationToken).ConfigureAwait(false);
                            Logger.LogInformation("Checkpointing for partition {partitionId} success code {successCode}", partitionContext.PartitionId, checkpointResult);
                        }
                        catch (Exception e)
                        {
                            Logger.LogError(e, "Checkpointing for partition {partitionId} raised an exception", partitionContext.PartitionId);
                            throw;
                        }
                    }
                }
            }
        }
        #endregion
    }
}
