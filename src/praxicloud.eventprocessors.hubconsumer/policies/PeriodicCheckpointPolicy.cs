// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.policies
{
    #region Using Clauses
    using Azure.Messaging.EventHubs;
    using System;
    #endregion

    /// <summary>
    /// Checkpoints based on a message count or time interval, whichever comes first
    /// </summary>
    public sealed class PeriodicCheckpointPolicy : CheckpointPolicy
    {
        #region Variables
        /// <summary>
        /// The number of messages to process between checkpoint operations
        /// </summary>
        private readonly int _messageInterval;

        /// <summary>
        /// The time to wait between checkpointing
        /// </summary>
        private readonly TimeSpan _timeInterval;

        /// <summary>
        /// The next message count that should be checkpointed
        /// </summary>
        private long _nextMessageCount;

        /// <summary>
        /// The next time checkpointing should occur
        /// </summary>
        private DateTimeOffset _nextCheckpointTime;
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="messageInterval">The number of messages to process between checkpoint operations</param>
        /// <param name="timeInterval">The time to wait between checkpointing</param>
        public PeriodicCheckpointPolicy(int messageInterval, TimeSpan timeInterval)
        {
            _messageInterval = Math.Max(messageInterval, 1);
            _timeInterval = TimeSpan.FromMilliseconds(Math.Max(timeInterval.TotalMilliseconds, 1.0));

            _nextMessageCount = _messageInterval;
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
            _nextMessageCount = messageCount + _messageInterval;
        }

        /// <inheritdoc />
        public override bool ShouldCheckpoint(long messageCount)
        {
            return (messageCount > _nextMessageCount || DateTimeOffset.UtcNow >= _nextCheckpointTime);
        }
        #endregion
    }
}
