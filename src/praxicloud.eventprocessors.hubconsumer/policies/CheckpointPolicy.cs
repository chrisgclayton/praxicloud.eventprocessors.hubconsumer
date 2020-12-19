// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.policies
{
    #region Using Clauses
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.metrics;
    using praxicloud.core.security;
    #endregion

    /// <summary>
    /// A base checkpointing policy that performs the core functions such as tracking message count
    /// </summary>
    public abstract class CheckpointPolicy : ICheckpointPolicy, IDisposable
    {
        #region Variables
        /// <summary>
        /// A metric container that counts the number of time the policy has been executed 
        /// </summary>
        private ICounter _checkpointExecutedCounter;

        /// <summary>
        /// A metric counter that counts the number of errors that have occurred while checkpointing
        /// </summary>
        private ICounter _checkpointErrorCounter;

        /// <summary>
        /// A metric container that counts the number of time the policy has been invoked
        /// </summary>
        private ICounter _checkpointRequestedCounter;

        /// <summary>
        /// The number of messages that have been processed
        /// </summary>
        private long _messageCount;

        /// <summary>
        /// A control used to ensure multiple calls to checkpoint are not performed at once
        /// </summary>
        private readonly SemaphoreSlim _checkpointControl = new SemaphoreSlim(1);

        /// <summary>
        /// The number of times the instance has been disposed of
        /// </summary>
        private long _disposalCount;

        /// <summary>
        /// The last sequence number that was checkpointed
        /// </summary>
        private long _lastSequenceNumber = -1;
        #endregion
        #region Properties
        /// <summary>
        /// Used to write debugging and diagnostics information
        /// </summary>
        protected ILogger Logger { get; private set; }

        /// <summary>
        /// A factory that is used to create metrics containers
        /// </summary>
        protected IMetricFactory MetricFactory { get; private set; }

        /// <summary>
        /// The partition context of the policy
        /// </summary>
        protected ProcessorPartitionContext Context { get; private set; }

        /// <summary>
        /// The number of messages that have been processed
        /// </summary>
        public long MessageCount => _messageCount;

        /// <summary>
        /// The name of the policy
        /// </summary>
        public abstract string Name { get; }

        /// <summary>
        /// The partition Id
        /// </summary>
        public string ParitionId => Context.PartitionId;

        /// <summary>
        /// The last sequence number that was successfully checkpointed
        /// </summary>
        public long SequenceNumber => _lastSequenceNumber;
        #endregion
        #region Methods
        /// <inheritdoc />
        public virtual Task<bool> InitializeAsync(ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext context, CancellationToken cancellationToken)
        {
            Guard.NotNull(nameof(logger), logger);
            Guard.NotNull(nameof(metricFactory), metricFactory);
            Guard.NotNull(nameof(context), context);

            Logger = logger;

            using (Logger.BeginScope("Initializing Policy Manager"))
            {
                MetricFactory = metricFactory;
                Context = context;

                Logger.LogInformation("Initializing policy manager {name}", Name);

                _checkpointExecutedCounter = MetricFactory.CreateCounter("cpol-execution-count", "Counts the number of times a checkpoint has been performed", false, new string[0]);
                _checkpointErrorCounter = MetricFactory.CreateCounter("cpol-error-count", "Counts the number of times a checkpoint has generated an error", false, new string[0]);
                _checkpointRequestedCounter = MetricFactory.CreateCounter("cpol-requested-count", "Counts the number of times a checkpoint has been requested", false, new string[0]);
            }

            return Task.FromResult(true);
        }

        /// <inheritdoc />
        public virtual async Task<bool> CheckpointAsync(EventData eventData, bool force, CancellationToken cancellationToken)
        {
            Guard.NotNull(nameof(eventData), eventData);

            var checkpointed = false;

            using(Logger.BeginScope("Checkpoint Requested"))
            {
                _checkpointRequestedCounter.Increment();
                Logger.LogInformation("Checkpoint requested for partition {partitionId}.", Context.PartitionId);

                var messageCount = _messageCount;
                var shouldCheckpoint = ShouldCheckpoint(messageCount);
                var sequenceNumberChanged = _lastSequenceNumber < eventData.SequenceNumber;

                Logger.LogDebug("Checkpoint decision for for partition {partitionId}, force = {force}, shouldCheck = {shouldCheckpoint}, sequence number changed = {sequenceNumberChanged}.", Context.PartitionId, force, shouldCheckpoint, sequenceNumberChanged);

                if ((force || shouldCheckpoint) && sequenceNumberChanged)
                {
                    var lockAcquired = false;
                    await _checkpointControl.WaitAsync(cancellationToken).ConfigureAwait(false);

                    try
                    {
                        if (_lastSequenceNumber < eventData.SequenceNumber)
                        {
                            Logger.LogDebug("Partition {partitionId} sequence numb", force);

                            lockAcquired = true;
                            _checkpointExecutedCounter.Increment();
                            checkpointed = await Context.CheckpointAsync(eventData, cancellationToken).ConfigureAwait(false);

                            if (checkpointed)
                            {
                                Logger.LogDebug("Checkpoint attempt was successful");
                                _lastSequenceNumber = eventData.SequenceNumber;
                                CheckpointPerformed(eventData, force, messageCount);
                            }
                            else
                            {
                                Logger.LogInformation("Checkpoint attempt was not successful");
                            }
                        }
                    }
                    catch(Exception e)
                    {
                        _checkpointErrorCounter.Increment();
                        Logger.LogError(e, "Error checkpointing on partition {partitionId}", Context.PartitionId);
                    }
                    finally
                    {
                        if(lockAcquired)
                        {
                            _checkpointControl.Release();
                        }
                    }
                }
                else
                {
                    // Consider checkpointing successful because it was not required
                    checkpointed = true;
                }
            }

            return checkpointed;
        }

        /// <inheritdoc />
        public void IncrementBy(int count)
        {
            Interlocked.Add(ref _messageCount, count);
        }

        /// <summary>
        /// Invoked when checkpoint is invoked and determines if the conditions to checkpoint have been met.
        /// </summary>
        /// <returns>True if checkpointing should occur</returns>
        public abstract bool ShouldCheckpoint(long messageCount);

        /// <summary>
        /// True if a checkpoint was performed
        /// </summary>
        /// <param name="eventData">The event data that was checkpointed</param>
        /// <param name="force">True if the checkpoint was forced</param>
        public abstract void CheckpointPerformed(EventData eventData, bool force, long messageCount);
        #endregion
        #region Safe Disposal pattern
        /// <summary>
        /// Releases resources
        /// </summary>
        ~CheckpointPolicy()
        {
            Dispose(false);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// A common disposal pattern
        /// </summary>
        /// <param name="isDisposing">True if disposing</param>
        protected virtual void Dispose(bool isDisposing)
        {
            if(Interlocked.Increment(ref _disposalCount) == 1)
            {
                if (isDisposing) GC.SuppressFinalize(this);
                _checkpointControl.Dispose();
            }
        }
        #endregion
    }
}
