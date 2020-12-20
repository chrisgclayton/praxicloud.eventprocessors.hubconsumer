// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.processors
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
    using praxicloud.core.security;
    using praxicloud.eventprocessors.hubconsumer.poisoned;
    using praxicloud.eventprocessors.hubconsumer.policies;
    using praxicloud.eventprocessors.hubconsumer.processors;
    #endregion

    /// <summary>
    /// A processor base that handles checkpointing and provides the tools to check for poison messages. The derived type does not have to implement any checkpointing logic, the policy will handle this
    /// </summary>
    public abstract class CheckpointingProcessor : Processor, IEventBatchProcessor
    {
        #region Variables
        /// <summary>
        /// The checkpoint manager to use for controlling how often and the conditions checkpointing should occur.
        /// </summary>
        private readonly ICheckpointPolicy _checkpointPolicy;

        /// <summary>
        /// The poison message monitor to use for bad message protections
        /// </summary>
        private readonly IPoisonedMonitor _poisonMonitor;

        /// <summary>
        /// The metric factory in use to create metric containers
        /// </summary>
        private IMetricFactory _metricFactory;

        /// <summary>
        /// The partition context that this processor is responsible for
        /// </summary>
        private ProcessorPartitionContext _partitionContext;

        /// <summary>
        /// True if the first message has been checked for poisoned status
        /// </summary>
        private bool _poisonCheckCompleted = false;

        /// <summary>
        /// The number of messages that have beenr received
        /// </summary>
        private long _messageCount;

        /// <summary>
        /// The event data that the processor should checkpoint to
        /// </summary>
        private EventData _checkpointTo = null;
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="messageInterval">The number of messages to process between checkpoint operations</param>
        /// <param name="timeInterval">The time to wait between checkpointing</param>
        /// <param name="poisonMonitor">If a poison message monitor is not provided to test for bad messages at startup the NoopPoisonMonitor instance will be used</param>
        protected CheckpointingProcessor(int messageInterval, TimeSpan timeInterval, IPoisonedMonitor poisonMonitor = null) : this(new PeriodicCheckpointPolicy(messageInterval, timeInterval), poisonMonitor)
        {
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="messageInterval">The number of messages to process between checkpoint operations</param>
        /// <param name="timeInterval">The time to wait between checkpointing</param>
        /// <param name="poisonMonitor">If a poison message monitor is not provided to test for bad messages at startup the NoopPoisonMonitor instance will be used</param>
        protected CheckpointingProcessor(ICheckpointPolicy checkpointPolicy, IPoisonedMonitor poisonMonitor = null)
        {
            Guard.NotNull(nameof(checkpointPolicy), checkpointPolicy);

            _checkpointPolicy = checkpointPolicy;
            _poisonMonitor = poisonMonitor ?? NoopPoisonedMonitor.Instance;
        }
        #endregion
        #region Properties
        /// <summary>
        /// The metric factory in use to create metric containers
        /// </summary>
        protected IMetricFactory MetricFactory => _metricFactory;

        /// <summary>
        /// The partition context that this processor is responsible for
        /// </summary>
        protected ProcessorPartitionContext Context => _partitionContext;

        /// <summary>
        /// The partition id that the processor is responsible for
        /// </summary>
        public string PartitionId => _partitionContext.PartitionId;

        /// <summary>
        /// True if a message has been checked for poison status since the processor started
        /// </summary>
        public bool PoisonCheckCompleted => _poisonCheckCompleted;

        /// <summary>
        /// The number of messages that have beenr received
        /// </summary>
        public long MessageCount => _messageCount;
        #endregion
        #region Methods
        /// <summary>
        /// Invoked when the instance is being initialized, prior to the processor starting.
        /// </summary>
        protected virtual Task InitializeInstanceAsync()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Implement to provide custom handling of poison messages, by default it will be logged and disposed of
        /// </summary>
        /// <param name="data">The message that is poisoned</param>
        /// <param name="partitionContext">A partition context associated with the current processor</param>
        /// <param name="cancellationToken">A token to monitor for abort and cancellation requests</param>
        /// <returns>True if successfully completed the handling. If it fails the handler will be invoked again to a maximum of 3 times total</returns>
        protected virtual Task<bool> HandlePoisonMessageAsync(EventData data, ProcessorPartitionContext partitionContext, CancellationToken cancellationToken)
        {
            Logger.LogWarning("Partition {partitionId}, consider poison message Sequence Number {sequenceNumber} handled", partitionContext.PartitionId, data.SequenceNumber);

            return Task.FromResult(true);
        }

        /// <summary>
        /// Processes a batch of messages. The first message ever processed by this method should check for poison messages. When an event is completed successfully update the checkpoint to message. 
        /// </summary>
        /// <param name="events">The event enumeration that is not null but may be empty to process.</param>
        /// <param name="cancellationToken">A token to monitor for abort and cancellation requests</param>
        protected abstract Task ProcessBatchAsync(IEnumerable<EventData> events, CancellationToken cancellationToken);

        /// <summary>
        /// Provided to allow derived types to add additional logic when the partition processor is stopping
        /// </summary>
        /// <param name="reason">The reason to stop (shutdown or lease lost)</param>
        /// <param name="cancellationToken">A token to monitor for abort and cancellation requests</param>
        protected virtual Task ProcessorStoppingAsync(ProcessingStoppedReason reason, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }


        /// <inheritdoc />
        public override sealed async Task InitializeAsync(ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext partitionContext)
        {
            _metricFactory = metricFactory;
            _partitionContext = partitionContext;

            await base.InitializeAsync(logger, metricFactory, partitionContext).ConfigureAwait(false);
            await _checkpointPolicy.InitializeAsync(Logger, metricFactory, partitionContext, CancellationToken.None).ConfigureAwait(false);
            await InitializeInstanceAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Used by the derived type to set the event data that can safely be checkpointed to
        /// </summary>
        /// <param name="eventData">The event data to be checkpointed to</param>
        protected void SetCheckpointTo(EventData eventData)
        {
            if (_checkpointTo == null || _checkpointTo.SequenceNumber < eventData.SequenceNumber) _checkpointTo = eventData;
        }

        /// <summary>
        /// Used by the derived type to incremenet the message counter
        /// </summary>
        /// <param name="incrementBy">The number of events to increment the counter by</param>
        protected void IncrementMessageCount(int incrementBy)
        {
            MessageCounter.IncrementBy(incrementBy);
            _messageCount += incrementBy;
            _checkpointPolicy.IncrementBy(incrementBy);
        }

        /// <summary>
        /// Called by the derived type to check if the first received message is poisoned
        /// </summary>
        /// <param name="eventData">The event data message</param>
        /// <returns>True if the message provided has been identified as poisoned, null if it failed testing</returns>
        protected async Task<bool?> IsMessagePoisoned(EventData eventData, CancellationToken cancellationToken)
        {
            bool? isPoisoned = null;

            try
            {
                isPoisoned = !(await UpdatePoisonMonitorStatusAsync(eventData, _partitionContext, cancellationToken).ConfigureAwait(false));
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Error testing message, assume it is safe to stop loss of message or blocking on successful compmletion");
                isPoisoned = null;
            }
            finally
            {
                _poisonCheckCompleted = true;
            }

            return isPoisoned;
        }

        /// <inheritdoc />
        public override sealed async Task PartitionStopAsync(ProcessorPartitionContext partitionContext, ProcessingStoppedReason reason, CancellationToken cancellationToken)
        {
            await base.PartitionStopAsync(partitionContext, reason, cancellationToken).ConfigureAwait(false);

            using (Logger.BeginScope("Processor stopping"))
            {
                if (_checkpointTo == null)
                {
                    Logger.LogInformation("Shutting down but no checkpoint data available on partition {partitionId}", partitionContext.PartitionId);
                }
                else if (reason == ProcessingStoppedReason.OwnershipLost)
                {
                    Logger.LogWarning("Cannot checkpoint, ownership was lost for partition {partitionId}, last known sequence number: {checkpointTo}", partitionContext.PartitionId, _checkpointTo.SequenceNumber);
                }
                else
                {
                    try
                    {
                        Logger.LogInformation("Graceful shutdown on partition {partitionId}, forcing checkpoint to sequence number {sequenceNumber}", partitionContext.PartitionId, _checkpointTo.SequenceNumber);

                        var tokenSource = new CancellationTokenSource(3000);
                        var checkpointSuccess = await _checkpointPolicy.CheckpointAsync(_checkpointTo, true, tokenSource.Token).ConfigureAwait(false);
                        Logger.LogInformation("Completed checkpoint on partition {partitionId} result {result}", partitionContext.PartitionId, checkpointSuccess);
                    }
                    catch (Exception e)
                    {
                        Logger.LogError(e, "Error checkpointing on partition {partitionId} during graceful shutdown, duplicate data expected", partitionContext.PartitionId);
                    }
                }

                var derivedTokenSource = new CancellationTokenSource(3000);
                await ProcessorStoppingAsync(reason, derivedTokenSource.Token).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public async Task PartitionProcessAsync(IEnumerable<EventData> events, ProcessorPartitionContext partitionContext, CancellationToken cancellationToken)
        {
            using (Logger.BeginScope("Processing Event Batch"))
            {
                if (events != null)
                {
                    try
                    {
                        if (!PoisonCheckCompleted)
                        {
                            // Check if the are any evens in the list
                            var messageEnumerator = events.GetEnumerator();
                            var hasMessages = messageEnumerator.MoveNext();

                            if (hasMessages)
                            {
                                if (await IsMessagePoisoned(messageEnumerator.Current, cancellationToken).ConfigureAwait(false) ?? false)
                                {
                                    Logger.LogWarning("Removing poison message, providing remainder of batch on partition {partitionId} for processing sequence number {sequenceNumber}.", partitionContext.PartitionId, messageEnumerator.Current.SequenceNumber);
                                    var updatedList = events.ToList();
                                    updatedList.RemoveAt(0);
                                    events = updatedList;
                                    Logger.LogWarning("Removed poison message, providing remainder of batch on partition {partitionId} for processing {count} message remaining.", partitionContext.PartitionId, updatedList.Count);
                                }
                            }
                        }

                        Logger.LogDebug("Starting custom processing of batch for partition {partitionId}", partitionContext.PartitionId);
                        await ProcessBatchAsync(events, cancellationToken).ConfigureAwait(false);

                        if (_checkpointTo != null)
                        {
                            Logger.LogDebug("Starting policy checkpoint for partition {partitionId}", partitionContext.PartitionId);
                            var checkpointResult = await _checkpointPolicy.CheckpointAsync(_checkpointTo, false, cancellationToken).ConfigureAwait(false);
                            Logger.LogDebug("Completed policy checkpoint for partition {partitionId}, result: {result}", partitionContext.PartitionId, checkpointResult);
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.LogError(e, "Error raised by customer batch processor on partition {partitionId}", partitionContext.PartitionId);
                        throw;
                    }
                }
                else if (_checkpointTo != default && _checkpointTo.SequenceNumber > _checkpointPolicy.SequenceNumber)
                {
                    if (_checkpointTo != null)
                    {
                        Logger.LogDebug("No events received, starting policy checkpoint for partition {partitionId}", partitionContext.PartitionId);
                        var checkpointResult = await _checkpointPolicy.CheckpointAsync(_checkpointTo, false, cancellationToken).ConfigureAwait(false);
                        Logger.LogDebug("Completed policy checkpoint for partition {partitionId}, result: {result}", partitionContext.PartitionId, checkpointResult);
                    }
                }
            }
        }

        /// <summary>
        /// Updates the poison message information and checks the provided message for poison status, This should only be called on the first message received in a batch, all others should always return false
        /// </summary>
        /// <param name="data">The event data to check for poisoned message status</param>
        /// <param name="partitionContext">The partition context associated with the processor</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>True if the message is safe to process, false if poisoned</returns>
        protected async Task<bool> UpdatePoisonMonitorStatusAsync(EventData data, ProcessorPartitionContext partitionContext, CancellationToken cancellationToken)
        {
            var shouldProcess = false;

            try
            {
                Logger.LogInformation("Partition {partitionId} checking if Sequence Number {sequenceNumber} is poisoned", partitionContext.PartitionId, data.SequenceNumber);

                if (await _poisonMonitor.IsPoisonedMessageAsync(partitionContext.PartitionId, data.SequenceNumber, cancellationToken).ConfigureAwait(false))
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
                        await _checkpointPolicy.CheckpointAsync(data, true, cancellationToken).ConfigureAwait(false);
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
                            poisonData = await _poisonMonitor.GetPoisonDataAsync(partitionContext.PartitionId, cancellationToken).ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            Logger.LogError(e, "Error retrieving poison message data for partition {partitionId}", partitionContext.PartitionId);
                        }
                    }

                    if (poisonData.SequenceNumber == -1)
                    {
                        poisonData.SequenceNumber = data.SequenceNumber;
                        poisonData.ReceiveCount = 0;
                    }
                    else if (poisonData.SequenceNumber == data.SequenceNumber)
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
                            updateSuccess = await _poisonMonitor.UpdatePoisonDataAsync(poisonData, cancellationToken).ConfigureAwait(false);
                        }
                        catch (Exception e)
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
        #endregion
    }
}
