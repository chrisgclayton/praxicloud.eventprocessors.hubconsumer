// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.sample
{
    #region Using Clauses
    using System;
    using System.Collections.Generic;
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
        private readonly IExecutionPartitioner _partitioner;
        private readonly IConcurrencyManager _concurrencyManager;

        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="messageInterval">The number of messages to process between checkpoint operations</param>
        /// <param name="timeInterval">The time to wait between checkpointing</param>
        /// <param name="poisonMonitor">If a poison message monitor is not provided to test for bad messages at startup the NoopPoisonMonitor instance will be used</param>
        public DemoProcessor(IExecutionPartitioner partitioner, IConcurrencyManager concurrencyManager, int messageInterval, TimeSpan timeInterval, IPoisonedMonitor poisonMonitor = null) : base(messageInterval, timeInterval, poisonMonitor)
        {
            _partitioner = partitioner;
            _concurrencyManager = concurrencyManager;
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="messageInterval">The number of messages to process between checkpoint operations</param>
        /// <param name="timeInterval">The time to wait between checkpointing</param>
        /// <param name="poisonMonitor">If a poison message monitor is not provided to test for bad messages at startup the NoopPoisonMonitor instance will be used</param>
        public DemoProcessor(IExecutionPartitioner partitioner, IConcurrencyManager concurrencyManager,  ICheckpointPolicy checkpointPolicy, IPoisonedMonitor poisonMonitor = null) : base(checkpointPolicy, poisonMonitor)
        {
            _partitioner = partitioner;
            _concurrencyManager = concurrencyManager;
        }
        #endregion

        /// <inheritdoc />
        protected override async Task ProcessBatchAsync(IEnumerable<EventData> events, CancellationToken cancellationToken)
        {
            var messageCounter = 0;
            EventData lastData = null;

            foreach (var data in events)
            {
                messageCounter++;
                
                while(!await _concurrencyManager.ScheduleAsync(data, Task.FromResult(data), TimeSpan.FromMilliseconds(1000), cancellationToken))
                {
                    await Task.Delay(1).ConfigureAwait(false);
                }

                lastData = data;
            }

            if(lastData != null)
            {
                try
                {

                    var completedTasks = await _concurrencyManager.WhenAllAsync(TimeSpan.FromSeconds(90), cancellationToken).ConfigureAwait(false);

                    foreach(var item in completedTasks)
                    {
                        if(item.IsCompletedSuccessfully)
                        {
                            var abc = item.Result;
                        }
                        else
                        {
                            var abc = item.Result;

                        }
                    }
                }
                catch(Exception e)
                {
                    Logger.LogError(e, "Error processing task");
                }                

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
