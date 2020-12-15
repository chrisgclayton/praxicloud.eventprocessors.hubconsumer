// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer
{
    #region Using Clauses
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    #endregion

    /// <summary>
    /// A batch event processor
    /// </summary>
    public interface IEventBatchProcessor : IProcessor
    {
        #region Methods
        /// <summary>
        /// Invoked when a batch of messages is received from the hub
        /// </summary>
        /// <param name="events">The batch received which may be null or empty in the event of receive timeout</param>
        /// <param name="partitionContext">The partition context that is also used for checkpointing etc.</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        Task PartitionProcessAsync(IEnumerable<EventData> events, ProcessorPartitionContext partitionContext, CancellationToken cancellationToken);
        #endregion
    }
}
