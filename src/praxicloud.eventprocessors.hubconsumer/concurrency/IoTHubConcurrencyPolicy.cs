// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.concurrency
{
    #region Using Clauses
    using praxicloud.eventprocessors.hubconsumer.partitioners;
    #endregion

    /// <summary>
    /// A concurrency policy specifically targetting IoT Hub and the use of the device id as the partitioner. 
    /// </summary>
    public sealed class IoTHubConcurrencyPolicy : PartitionedMaximumConcurrencyPolicy
    {
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="maximumDegreeOfParallelism">The maximum degree of parallelism allowed</param>
        public IoTHubConcurrencyPolicy(short maximumDegreeOfParallelism) : base(maximumDegreeOfParallelism, new DefaultIoTHubPartitioner())
        {
        }
    }
}
