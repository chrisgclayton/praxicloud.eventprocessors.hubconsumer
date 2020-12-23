// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.partitioners
{
    #region Using Clauses
    using Azure.Messaging.EventHubs;
    #endregion

    /// <summary>
    /// A partioner that uses the default Event Hub partition key
    /// </summary>
    public sealed class DefaultEventHubPartitioner : IExecutionPartitioner
    {
        #region Properties
        /// <inheritdoc />
        public bool IsCaseSensitive => true;
        #endregion

        /// <inheritdoc />
        public string GetPartition(EventData data)
        {            
            return data.PartitionKey;
        }
    }
}
