// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.policies
{
    #region Using Clauses
    using Azure.Messaging.EventHubs;
    using System;
    #endregion

    /// <summary>
    /// Checkpoints based on a time interval
    /// </summary>
    public sealed class IntervalCheckpointPolicy : CheckpointPolicy
    {
        #region Variables
        /// <summary>
        /// The time to wait between checkpointing
        /// </summary>
        private readonly TimeSpan _timeInterval;

        /// <summary>
        /// The next time checkpointing should occur
        /// </summary>
        private DateTimeOffset _nextCheckpointTime;
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="timeInterval">The time to wait between checkpointing</param>
        public IntervalCheckpointPolicy(TimeSpan timeInterval)
        {
            _timeInterval = TimeSpan.FromMilliseconds(Math.Max(timeInterval.TotalMilliseconds, 1.0));

            _nextCheckpointTime = DateTimeOffset.UtcNow.Add(_timeInterval);
        }
        #endregion
        #region Properties
        /// <inheritdoc />
        public override string Name => nameof(PeriodicCheckpointPolicy);
        #endregion
        #region Methods
        /// <inheritdoc />
        public override void CheckpointPerformed(EventData eventData, bool force, long messageCount)
        {
            _nextCheckpointTime = DateTime.UtcNow.Add(_timeInterval);
        }

        /// <inheritdoc />
        public override bool ShouldCheckpoint(long messageCount)
        {
            return DateTimeOffset.UtcNow >= _nextCheckpointTime;
        }
        #endregion
    }
}
