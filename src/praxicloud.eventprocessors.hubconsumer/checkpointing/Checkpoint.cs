// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.checkpointing
{
    #region Using Clauses
    using Azure.Messaging.EventHubs.Primitives;
    #endregion

    /// <summary>
    /// A checkpoint that includes the offset nd sequence number
    /// </summary>
    public class Checkpoint : EventProcessorCheckpoint
    {
        #region Properties
        /// <summary>
        /// The sequence number or null
        /// </summary>
        public long? SequenceNumber { get; set; }
        #endregion
    }
}
