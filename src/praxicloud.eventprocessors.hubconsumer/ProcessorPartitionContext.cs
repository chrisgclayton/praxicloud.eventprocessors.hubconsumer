// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer
{
    #region Using Clauses
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Consumer;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.metrics;
    #endregion

    /// <summary>
    /// The partition context used by the processor types
    /// </summary>
    public class ProcessorPartitionContext : PartitionContext
    {
        #region Delegates
        /// <summary>
        /// A method used to retrieve the partition properties
        /// </summary>
        /// <returns>The event properties</returns>
        public delegate LastEnqueuedEventProperties LastPropertiesReader();
        #endregion
        #region Variables
        /// <summary>
        /// A method used to retrieve the metadata associated with the property
        /// </summary>
        private LastPropertiesReader _readLastEnqueuedEventProperties;

        /// <summary>
        /// The checkpoint manager
        /// </summary>
        private readonly ICheckpointManager _checkpointManager;

        /// <summary>
        /// A logger used to write debugging and diagnostics to
        /// </summary>
        private readonly ILogger _logger;

        /// <summary>
        /// Tracks the number of times the checkpoint as been executed
        /// </summary>
        private readonly ICounter _checkpointCounter;

        /// <summary>
        /// Tracks the time taken to perform checkpoints
        /// </summary>
        private readonly ISummary _checkpointTiming;
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="metricFactory">The factory to create metrics containers from.</param>
        /// <param name="partitionId">The identifier of the partition that the context represents.</param>
        /// <param name="readLastEnqueuedEventProperties">A function that can be used to read the last enqueued event properties for the partition.</param>        
        /// <param name="checkpointManager">The checkpoint manager to used to create the checkpoint</param>
        internal ProcessorPartitionContext(ILogger logger, IMetricFactory metricFactory, string partitionId, LastPropertiesReader readLastEnqueuedEventProperties, ICheckpointManager checkpointManager) : base(partitionId)
        {
            _logger = logger;
            _checkpointManager = checkpointManager;
            _readLastEnqueuedEventProperties = readLastEnqueuedEventProperties;

            _checkpointCounter = metricFactory.CreateCounter($"ppc-checkpoint-counter-{partitionId}", $"The number of times that the checkpoint has been called for partition {partitionId}", false, new string[0]);
            _checkpointTiming = metricFactory.CreateSummary($"ppc-checkpoint-timing-{partitionId}", $"The time taken to perform checkpoints for partition {partitionId}", 10, false, new string[0]);
        }
        #endregion
        #region Properties
        /// <summary>
        /// A set of information about the last enqueued event of a partition, not available for the
        /// empty context.
        /// </summary>
        /// <returns>The set of properties for the last event that was enqueued to the partition.</returns>
        public override LastEnqueuedEventProperties ReadLastEnqueuedEventProperties() => _readLastEnqueuedEventProperties();
        #endregion
        #region Methods
        /// <summary>
        /// Updates the checkpoint for the partition
        /// </summary>
        /// <param name="eventData">The event data to checkpoint to</param>
        /// <param name="cancellationToken">A token to monitor for abort requests.</param>
        /// <returns>True if the checkpointing was successful.</returns>
        public async Task<bool> CheckpointAsync(EventData eventData, CancellationToken cancellationToken)
        {
            var success = false;

            try
            {
                _logger.LogInformation("Checkpointing for partition {partitionId}", PartitionId);

                _checkpointCounter.Increment();

                using (_checkpointTiming.Time())
                {
                    await _checkpointManager.UpdateCheckpointAsync(eventData, this, cancellationToken).ConfigureAwait(false);
                }

                success = true;
                _logger.LogDebug("Finished checkpointing for partition {partitionId}", PartitionId);
            }
            catch(Exception e)
            {
                _logger.LogError(e, "Error checkpointing for partition {partitionId}", PartitionId);
            }

            return success;
        }
        #endregion
    }
}
