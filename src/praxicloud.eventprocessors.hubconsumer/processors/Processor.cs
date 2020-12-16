// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.processors
{
    #region Using Clauses
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs.Processor;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.metrics;
    #endregion

    /// <summary>
    /// A base processor class for simplification of common tasks
    /// </summary>
    public abstract class Processor : IProcessor
    {
        #region Properties
        /// <summary>
        /// A logger to write debugging and diagnostics information to
        /// </summary>
        protected ILogger Logger { get; set; }

        /// <summary>
        /// A metric container that counts the number of messages received
        /// </summary>
        protected ICounter MessageCounter { get; private set; }

        /// <summary>
        /// A metric counter that counts the number of errors that have occured
        /// </summary>
        protected ICounter ErrorCounter { get; private set; }
        #endregion
        #region Methods
        /// <inheritdoc />
        public virtual Task InitializeAsync(ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext partitionContext)
        {
            Logger = logger;

            using (Logger.BeginScope("Initialize batch processor"))
            {
                Logger.LogInformation("Initializing batch processor for partition {partitionId}", partitionContext.PartitionId);
                MessageCounter = metricFactory.CreateCounter($"hcp-message-count-{partitionContext.PartitionId}", "The number of messages that have been processed by partition {partitionContext.PartitionId}", false, new string[0]);
                ErrorCounter = metricFactory.CreateCounter($"hcp-error-count-{partitionContext.PartitionId}", "The number of errors that have been processed by partition {partitionContext.PartitionId}", false, new string[0]);
            }

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public virtual Task PartitionHandleErrorAsync(Exception exception, ProcessorPartitionContext partitionContext, string operationDescription, CancellationToken cancellationToken)
        {
            using (Logger.BeginScope("OnError"))
            {
                ErrorCounter.Increment();
                Logger.LogError(exception, "Erron partition {partitionId}, {operationDescription}", partitionContext.PartitionId, operationDescription);
            }

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public virtual Task PartitionInitializeAsync(ProcessorPartitionContext partitionContext, CancellationToken cancellationToken)
        {
            using (Logger.BeginScope("OnInitialize"))
            {
                Logger.LogInformation("Initializing partition {partitionId}", partitionContext.PartitionId);
            }

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public virtual Task PartitionStopAsync(ProcessorPartitionContext partitionContext, ProcessingStoppedReason reason, CancellationToken cancellationToken)
        {
            using (Logger.BeginScope("OnStop"))
            {
                Logger.LogInformation("Stopping partition {partitionId}, reason {reason}", partitionContext.PartitionId, Enum.GetName(typeof(ProcessingStoppedReason), reason));

                if (reason == ProcessingStoppedReason.Shutdown)
                {
                    Logger.LogInformation("Checkpointing in graceful shutdown for partition {partitionId}", partitionContext.PartitionId, Enum.GetName(typeof(ProcessingStoppedReason), reason));
                }
            }

            return Task.CompletedTask;
        }
        #endregion
    }
}
