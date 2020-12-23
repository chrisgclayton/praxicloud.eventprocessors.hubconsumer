// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.partitioners
{
    #region Using Clauses
    using Azure.Messaging.EventHubs;
    #endregion

    /// <summary>
    /// A partioner that uses the default IoT Hub device 
    /// </summary>
    public sealed class DefaultIoTHubPartitioner : IExecutionPartitioner
    {
        #region Constants
        private const string DeviceIdPropertyName = "iothub-connection-device-id";
        #endregion
        #region Properties
        /// <inheritdoc />
        public bool IsCaseSensitive => true;
        #endregion

        /// <inheritdoc />
        public string GetPartition(EventData data)
        {
            string partitionKey;

            if(data.SystemProperties.TryGetValue(DeviceIdPropertyName, out var partitionKeyValue))
            {
                partitionKey = partitionKeyValue as string;
            }
            else
            {
                partitionKey = null;
            }

            return partitionKey;
        }
    }
}
