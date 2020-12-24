// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.sample
{
    #region Using Clauses
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Processor;
    using Microsoft.Extensions.Logging;
    using praxicloud.eventprocessors.hubconsumer.processors;
    #endregion

    /// <summary>
    /// A demo processor that handles checkpointing and poison message processing
    /// </summary>
    public sealed class DemoProcessor : CheckpointingProcessor
    {
        private readonly IConcurrencyPolicy _concurrencyPolicy;

        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="concurrencyPolicy">The concurrency policy the processor uses</param>
        /// <param name="messageInterval">The number of messages to process between checkpoint operations</param>
        /// <param name="timeInterval">The time to wait between checkpointing</param>
        /// <param name="poisonMonitor">If a poison message monitor is not provided to test for bad messages at startup the NoopPoisonMonitor instance will be used</param>
        public DemoProcessor(IConcurrencyPolicy concurrencyPolicy, int messageInterval, TimeSpan timeInterval, IPoisonedMonitor poisonMonitor = null) : base(messageInterval, timeInterval, poisonMonitor)
        {
            _concurrencyPolicy = concurrencyPolicy;
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="concurrencyPolicy">The concurrency policy the processor uses</param>
        /// <param name="poisonMonitor">If a poison message monitor is not provided to test for bad messages at startup the NoopPoisonMonitor instance will be used</param>
        public DemoProcessor(IConcurrencyPolicy concurrencyPolicy,  ICheckpointPolicy checkpointPolicy, IPoisonedMonitor poisonMonitor = null) : base(checkpointPolicy, poisonMonitor)
        {
            _concurrencyPolicy = concurrencyPolicy;
        }
        #endregion

        private async Task<EventData> ProcessData(EventData data, object state, CancellationToken cancellationToken)
        {
            await Task.Delay(10).ConfigureAwait(false);

            return data;
        }

        /// <inheritdoc />
        protected override async Task ProcessBatchAsync(IEnumerable<EventData> events, CancellationToken cancellationToken)
        {
            var messageCounter = 0;
            EventData lastData = null;
            Dictionary<EventData, Task<EventData>> batchTasks = new Dictionary<EventData, Task<EventData>>(100);

            foreach (var data in events)
            {
                var executingTask = await _concurrencyPolicy.RunAsync(data, ProcessData, data.SequenceNumber, cancellationToken).ConfigureAwait(false);

                if (executingTask != null)
                {
                    messageCounter++;
                    batchTasks.Add(data, executingTask);
                }
                else
                {
                    await Task.Delay(5).ConfigureAwait(false);
                }                
            }

            foreach(var pair in batchTasks)
            {
                try
                {
                    var data = await pair.Value.ConfigureAwait(false);
                        
                    if(data.SequenceNumber > ((lastData?.SequenceNumber) ?? -1))
                    {
                        lastData = data;
                    }
                }
                catch(Exception e)
                {
                    Logger.LogError(e, "Error processing sequence number {sequenceNumber} on partition {partitionId}", pair.Key.SequenceNumber, Context.PartitionId);
                }
            }

            if (lastData != null)
            {
                SetCheckpointTo(lastData);
            }

            IncrementMessageCount(messageCounter);
        }

        /// <inheritdoc />
        protected override Task<bool> HandlePoisonMessageAsync(EventData data, ProcessorPartitionContext partitionContext, CancellationToken cancellationToken)
        {
            // Perform custom processing here for poisoned message, e.g. store to raw store
            return base.HandlePoisonMessageAsync(data, partitionContext, cancellationToken);
        }

        protected override Task ProcessorStoppingAsync(ProcessingStoppedReason reason, CancellationToken cancellationToken)
        {
            var baseMessageCount = base.MessageCount;

            Interlocked.Add(ref Program.TotalMessageCount, MessageCount);

            return base.ProcessorStoppingAsync(reason, cancellationToken);
        }
    }
}
