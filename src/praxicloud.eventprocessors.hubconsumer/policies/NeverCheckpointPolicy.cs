// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.policies
{
    #region Using Clauses
    using Azure.Messaging.EventHubs;
    using System;
    #endregion

    /// <summary>
    /// A checkpoint policy that never invokes checkpointing
    /// </summary>
    public sealed class NeverCheckpointPolicy : CheckpointPolicy
    {
        #region Properties
        /// <inheritdoc />
        public override string Name => throw new NotImplementedException();
        #endregion
        #region Methods
        /// <inheritdoc />
        public override void CheckpointPerformed(EventData eventData, bool force, long messageCount)
        {            

        }

        /// <inheritdoc />
        public override bool ShouldCheckpoint(long messageCount)
        {
            return false;
        }
        #endregion
    }
}
