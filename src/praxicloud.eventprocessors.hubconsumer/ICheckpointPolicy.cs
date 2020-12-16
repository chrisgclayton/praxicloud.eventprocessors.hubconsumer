// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer
{
    #region Using Clauses
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.metrics;
    #endregion

    /// <summary>
    /// A checkpointing policy used to control the frequency checkpoints occur for a specific partition
    /// </summary>
    public interface ICheckpointPolicy
    {
        #region Properties
        /// <summary>
        /// The number of messages that have been processed
        /// </summary>
        long MessageCount { get; }

        /// <summary>
        /// The name of the policy
        /// </summary>
        string Name { get; }

        /// <summary>
        /// The partition Id
        /// </summary>
        string ParitionId { get; }

        /// <summary>
        /// The last sequence number that was successfully checkpointed
        /// </summary>
        long SequenceNumber { get; }
        #endregion
        #region Methods
        /// <summary>
        /// Initializes the checkpoint policy with the associated partition context
        /// </summary>
        /// <param name="context">The partition context to checkpoint with</param>
        /// <param name="logger">A logger to write debugging and diagnostics information to</param>
        /// <param name="metricProvider">A factory to create metric containers from.</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>True if the checkpointing is successful</returns>
        Task<bool> InitializeAsync(ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext context, CancellationToken cancellationToken);

        /// <summary>
        /// Checkpoints if the policy conditions are met or ignores when they are not
        /// </summary>
        /// <param name="eventData">The event data to checkpoint to</param>
        /// <param name="force">True if the checkpoint should ignore the standard conditions and make an exception to checkpoint</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>True if the checkpoint was performed, false if not</returns>
        Task<bool> CheckpointAsync(EventData eventData, bool force, CancellationToken cancellationToken);

        /// <summary>
        /// Increments the message counter by the specified number
        /// </summary>
        /// <param name="count">The number of messages to increment the count by</param>
        void IncrementBy(int count);
        #endregion
    }
}
