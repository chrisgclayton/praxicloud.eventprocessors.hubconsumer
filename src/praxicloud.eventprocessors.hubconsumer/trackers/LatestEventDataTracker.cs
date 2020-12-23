// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.trackers
{
    #region Using Clauses
    using Azure.Messaging.EventHubs;
    using Nito.AsyncEx;
    using System.Threading;
    using System.Threading.Tasks;
    #endregion

    /// <summary>
    /// An event data tracker that always results not checkpointing
    /// </summary>
    public sealed class LatestEventDataTracker : IEventDataTracker
    {

        #region Variables
        /// <summary>
        /// A lock to control updates to the value
        /// </summary>
        private readonly object _valueControl = new object();

        /// <summary>
        /// The latest event data to checkpoint to
        /// </summary>
        private EventData _latestData = null;
        #endregion
        #region Methods
        /// <inheritdoc />
        public EventData GetCheckpointValue()
        {
            return _latestData;
        }

        /// <inheritdoc />
        public void Initialize(IEventDataTracker.IgnoreCheck ignoreHandler, long startSequenceNumber)
        {

        }

        /// <inheritdoc />
        public void TrackFailure(EventData data)
        {
            TrackEvent(data);
        }

        /// <inheritdoc />
        public void TrackSuccess(EventData data)
        {
            TrackEvent(data);
        }

        /// <summary>
        /// A common event data update method
        /// </summary>
        /// <param name="data">The event data being updated</param>
        private void TrackEvent(EventData data)
        {
            if ((_latestData?.SequenceNumber ?? -1) < data.SequenceNumber)
            {
                lock(_valueControl)
                {
                    if ((_latestData?.SequenceNumber ?? -1) < data.SequenceNumber)
                    {
                        _latestData = data;
                    }
                }
            }
        }
        #endregion
    }
}
