// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer
{
    #region Using Clauses
    using Azure.Messaging.EventHubs;
    #endregion

    /// <summary>
    /// A type that can be used to translate an EventData element into a logical partition for microbatching
    /// </summary>
    public interface IExecutionPartitioner
    {
        #region Properties
        /// <summary>
        /// True if the partition key is considered case sensitive
        /// </summary>
        bool IsCaseSensitive { get; }
        #endregion
        #region Methods
        /// <summary>
        /// Determines the partition of the event data provided
        /// </summary>
        /// <param name="data">The event data to analyze</param>
        /// <returns>The partition identifier</returns>
        string GetPartition(EventData data);
        #endregion
    }
}
