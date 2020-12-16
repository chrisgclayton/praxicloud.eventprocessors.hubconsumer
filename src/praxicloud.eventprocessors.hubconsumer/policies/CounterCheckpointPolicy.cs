// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.policies
{
    #region Using Clauses
    using Azure.Messaging.EventHubs;
    using System;
    #endregion

    /// <summary>
    /// Checkpoints based on a message count
    /// </summary>
    public sealed class CounterCheckpointPolicy : CheckpointPolicy
    {
        #region Properties
        /// <summary>
        /// The number of messages to process between checkpoint operations
        /// </summary>
        private readonly int _messageInterval;

        /// <summary>
        /// The next message count that should be checkpointed
        /// </summary>
        private long _nextMessageCount;
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="messageInterval">The number of messages to process between checkpoint operations</param>
        public CounterCheckpointPolicy(int messageInterval)
        {
            _messageInterval = Math.Max(messageInterval, 1);

            _nextMessageCount = _messageInterval;
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
            _nextMessageCount = messageCount + _messageInterval;
        }

        /// <inheritdoc />
        public override bool ShouldCheckpoint(long messageCount)
        {
            return (messageCount > _nextMessageCount);
        }
        #endregion
    }
}
