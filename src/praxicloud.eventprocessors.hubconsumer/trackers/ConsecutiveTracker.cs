// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.trackers
{
    #region Using Clauses
    using Azure.Messaging.EventHubs;
    using Nito.AsyncEx;
    using System;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Threading;
    using System.Threading.Tasks;
    #endregion

    /// <summary>
    /// An event data tracker that always results the last tracked event data even if failures or gaps exist
    /// </summary>
    public sealed class ConsecutiveTracker : IEventDataTracker
    {
        #region Variables
        /// <summary>
        /// The sequence number of the last checkpoint
        /// </summary>
        private long _lastCheckpoint;

        /// <summary>
        /// The last checkpointed data
        /// </summary>
        private EventData _lastData = default;

        /// <summary>
        /// A locking object to control access to the list
        /// </summary>
        private readonly object _listControl = new object();

        /// <summary>
        /// 
        /// </summary>
        private SortedList<long, EventData> _sortedList = new SortedList<long, EventData>();
        #endregion
        #region Methods
        /// <inheritdoc />
        public EventData GetCheckpointValue()
        {
            var checkpointData = _lastData;

            if (_sortedList.TryGetValue(_lastCheckpoint + 1, out var nextValue))
            {
                lock (_listControl)
                {
                    bool gapFound = false;
                    int index = 0;

                    while(!gapFound && index < _sortedList.Count)
                    {
                        if(_sortedList.Keys[index] == checkpointData.SequenceNumber)
                        {
                            checkpointData = _sortedList.Values[index];
                        }
                        else
                        {
                            gapFound = true;
                        }

                        index++;
                    }

                    for(var removalIndex = _lastCheckpoint; removalIndex <= checkpointData.SequenceNumber; removalIndex++)
                    {
                        _sortedList.Remove(removalIndex);
                    }

                    _lastData = checkpointData;
                    _lastCheckpoint = checkpointData.SequenceNumber;
                }
            }

            return checkpointData;
        }

        /// <inheritdoc />
        public void Initialize(IEventDataTracker.IgnoreCheck ignoreHandler, long startSequenceNumber)
        {
            _lastCheckpoint = startSequenceNumber;
        }

        /// <inheritdoc />
        public void TrackFailure(EventData data)
        {
            TrackData(data);
        }

        /// <inheritdoc />
        public void TrackSuccess(EventData data)
        {
            TrackData(data);
        }

        private void TrackData(EventData data)
        {
            lock (_listControl)
            {
                _sortedList.TryAdd(data.SequenceNumber, data);
            }
        }
        #endregion
    }
}
