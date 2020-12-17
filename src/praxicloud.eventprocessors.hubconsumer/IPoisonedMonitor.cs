// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer
{
    #region Using Clauses
    using Microsoft.Extensions.Logging;
    using praxicloud.core.metrics;
    using System.Threading;
    using System.Threading.Tasks;
    #endregion

    /// <summary>
    /// A monitor to check for poinson messages
    /// </summary>
    public interface IPoisonedMonitor
    {
        #region Properties
        /// <summary>
        /// The name of the poisoned monitor
        /// </summary>
        string Name { get; }
        #endregion
        #region Methods
        /// <summary>
        /// Creates the poison message store if it does not already exist
        /// </summary>
        /// <param name="client">THe fixed processor client using the store</param>
        /// <param name="options">Fixed processor options associated with the store being created</param>
        /// <param name="cancellationToken">A token to monitor for abort and cancellation requests</param>
        /// <returns>True if the store is created successfully or exists already</returns>
        Task<bool> CreateStoreIfNotExistsAsync(FixedProcessorClient client, FixedProcessorClientOptions options, CancellationToken cancellationToken);

        /// <summary>
        /// Initializes the poison monitor and prepares for use
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="metricFactory">A factory to create metric containers from</param>
        /// <param name="client">THe fixed processor client using the store</param>
        /// <param name="options">Fixed processor options associated with the store being created</param>
        /// <param name="cancellationToken">A token to monitor for abort and cancellation requests</param>
        /// <returns>True if initialized successfully</returns>
        Task<bool> InitializeAsync(FixedProcessorClient client, FixedProcessorClientOptions options, ILogger logger, IMetricFactory metricFactory, CancellationToken cancellationToken);

        /// <summary>
        /// Determines if the specified sequence number for the partition is considered a poison message
        /// </summary>
        /// <param name="partitionId">The partition id the check is associated with</param>
        /// <param name="sequenceNumber">The sequence number of the message to be validated</param>
        /// <param name="cancellationToken">A token to monitor for abort and cancellation requests</param>
        /// <returns>True if the message is poison based on the monitors logic</returns>
        Task<bool> IsPoisonedMessageAsync(string partitionId, long sequenceNumber, CancellationToken cancellationToken);

        /// <summary>
        /// Gets the poison data information for a partition
        /// </summary>
        /// <param name="partitionId">The partition id the check is associated with</param>
        /// <param name="cancellationToken">A token to monitor for abort and cancellation requests</param>
        /// <returns>The posion data information</returns>
        Task<PoisonData> GetPoisonDataAsync(string partitionId, CancellationToken cancellationToken);

        /// <summary>
        /// Updates the poison message information
        /// </summary>
        /// <param name="data">The poison message information</param>
        /// <param name="cancellationToken">A token to monitor for abort and cancellation requests</param>
        /// <returns>True if successfully processed</returns>
        Task<bool> UpdatePoisonDataAsync(PoisonData data, CancellationToken cancellationToken);
        #endregion
    }
}
