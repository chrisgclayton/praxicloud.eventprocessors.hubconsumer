// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer
{
    #region Using Clauses
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Consumer;
    using Azure.Messaging.EventHubs.Primitives;
    #endregion

    /// <summary>
    /// A checkpoint manager
    /// </summary>
    public interface ICheckpointManager
    {
        /// <summary>
        /// Create the store if it does not already exist
        /// </summary>
        /// <param name="options">Fixed processor options associated with the store being created</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>True if the store exists or was created successfully</returns>
        Task<bool> CreateStoreIfNotExistsAsync(FixedProcessorClientOptions options, CancellationToken cancellationToken);

        /// <summary>
        /// Initializes the manager
        /// </summary>
        /// <param name="client">THe fixed processor client using the store</param>
        /// <param name="options">Fixed processor options associated with the store being created</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>True if the manager is initialized successfully</returns>
        Task<bool> InitializeAsync(FixedProcessorClient client, FixedProcessorClientOptions options, CancellationToken cancellationToken); 

        /// <summary>
        /// Retrieves a list of all checkpoints for the processor in the store
        /// </summary>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>A list of all checkpoints in the store</returns>
        Task<IEnumerable<EventProcessorCheckpoint>> ListCheckpointsAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Adds or updates the checkpoint in the store
        /// </summary>
        /// <param name="eventData">The event data being checkpointed to</param>
        /// <param name="context">The partition context the checkpoint is associated with</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>True if the checkpoint was successful</returns>
        Task<bool> UpdateCheckpointAsync(EventData eventData, PartitionContext context, CancellationToken cancellationToken);
    }
}
