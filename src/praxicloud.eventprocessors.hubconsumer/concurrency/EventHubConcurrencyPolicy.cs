// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.concurrency
{
    #region Using Clauses
    using praxicloud.eventprocessors.hubconsumer.partitioners;
    #endregion

    /// <summary>
    /// A concurrency policy specifically targetting Event Hub and the use of the partition key as the partitioner. This will not work properly if the producer does not set the partition key.
    /// </summary>
    public sealed class EventHubConcurrencyPolicy : PartitionedMaximumConcurrencyPolicy
    {
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="maximumDegreeOfParallelism">The maximum degree of parallelism allowed</param>
        public EventHubConcurrencyPolicy(short maximumDegreeOfParallelism) : base(maximumDegreeOfParallelism, new DefaultEventHubPartitioner())
        {
        }
    }
}
