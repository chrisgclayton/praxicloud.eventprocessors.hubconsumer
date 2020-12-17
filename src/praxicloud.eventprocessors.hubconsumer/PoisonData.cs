// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer
{
    /// <summary>
    /// The number of times the event has been received at startup
    /// </summary>
    public sealed class PoisonData
    {
        #region Properties
        /// <summary>
        /// The partition id the data is associated with
        /// </summary>
        public string PartitionId { get; set; }

        /// <summary>
        /// The number of times that the message was received
        /// </summary>

        public int ReceiveCount { get; set; }

        /// <summary>
        /// The sequence number or null if start of stream
        /// </summary>
        public long? SequenceNumber { get; set; }
        #endregion
    }
}
