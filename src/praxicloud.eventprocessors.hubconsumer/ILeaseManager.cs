// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer
{
    #region Using Clauses
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs.Primitives;
    #endregion

    /// <summary>
    /// A manager that controls ownership of partitions
    /// </summary>
    public interface ILeaseManager
    {
        #region Methods
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
        /// Attempts to claim ownership of a list of partitions
        /// </summary>
        /// <param name="desiredOwnership">The list of ownership that ownership is being requested for</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>An enumeration of the partitions that were successfully claimed</returns>
        Task<IEnumerable<EventProcessorPartitionOwnership>> ClaimOwnershipAsync(IEnumerable<EventProcessorPartitionOwnership> desiredOwnership, CancellationToken cancellationToken);

        /// <summary>
        /// Retrieves a list of partitions that are owned
        /// </summary>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>An enumeration of the partitions that are currently owned</returns>
        Task<IEnumerable<EventProcessorPartitionOwnership>> ListOwnershipAsync(CancellationToken cancellationToken);
        #endregion
    }
}
