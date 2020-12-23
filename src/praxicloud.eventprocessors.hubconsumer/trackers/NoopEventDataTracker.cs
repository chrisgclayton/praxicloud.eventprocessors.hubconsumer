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
    /// An event data tracker that always results the last tracked event data even if failures or gaps exist
    /// </summary>
    public sealed class NoopEventDataTracker : IEventDataTracker
    {
        #region Methods
        /// <inheritdoc />
        public EventData GetCheckpointValue()
        {
            return default;
        }

        /// <inheritdoc />
        public void Initialize(IEventDataTracker.IgnoreCheck ignoreHandler, long startSequenceNumber)
        {

        }

        /// <inheritdoc />
        public void TrackFailure(EventData data)
        {
        }

        /// <inheritdoc />
        public void TrackSuccess(EventData data)
        {
        }
        #endregion

    }
}
