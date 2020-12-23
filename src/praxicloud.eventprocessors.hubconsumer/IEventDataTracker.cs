// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer
{
    #region Using Clauses
    using Azure.Messaging.EventHubs;
    using System.Threading;
    using System.Threading.Tasks;
    #endregion

    /// <summary>
    /// An event tracker that uses an algorithm to determine the latest checkpointable event data
    /// </summary>
    public interface IEventDataTracker
    {
        #region Delegates
        /// <summary>
        /// A method used to check if a sequence number or event should be ignored if failed or missing
        /// </summary>
        /// <param name="sequenceNumber">The sequence number of the event</param>
        /// <param name="data">The event data if failed or null if missing</param>
        /// <param name="repeatCount">The number of times the event has been repeated if it is failed</param>
        /// <returns>True if the event can be safely ignored, false to wait if missing or to wait for a replacement event outcome if being reprocessed by the processor</returns>
        delegate bool IgnoreCheck(long sequenceNumber, EventData data, int repeatCount);
        #endregion
        #region Methods
        /// <summary>
        /// Initializes the instance with the first sequence number in the list
        /// </summary>
        /// <param name="ignoreHandler">A handler used to check whether or not to ignore a specific failed event data</param>
        /// <param name="startSequenceNumber">The sequence number of the first event being processed</param>
        void Initialize(IgnoreCheck ignoreHandler, long startSequenceNumber);

        /// <summary>
        /// Records a successful event data
        /// </summary>
        /// <param name="data">Event data that was successfully processed</param>
        void TrackSuccess(EventData data);

        /// <summary>
        /// Records a failed event data
        /// </summary>
        /// <param name="data">Event data that was successfully processed</param>
        void TrackFailure(EventData data);

        /// <summary>
        /// Analyzes the failed, missing and successfully processed data to determine the latest event data to process. If no messages are received it will return null. Once checkpointing is possible for the data all 
        /// previous data will be removed from tracking.
        /// </summary>
        /// <returns>Null or the latest event data to checkpoint to</returns>
        EventData GetCheckpointValue();
        #endregion
    }
}
