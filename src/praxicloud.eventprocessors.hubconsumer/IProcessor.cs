// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer
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
    /// The core interface for an event processor
    /// </summary>
    public interface IProcessor
    {
        #region Methods
        /// <summary>
        /// A method that is invoked when a new instance is created for the partition
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="partitionContext">The partition context that is also used for checkpointing etc.</param>
        /// <param name="metricFactory">A factory to create metric containers from</param>
        Task InitializeAsync(ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext partitionContext);

        /// <summary>
        /// Raised when a partition has an exception
        /// </summary>
        /// <param name="exception">The exception that was raised</param>
        /// <param name="partitionContext">The partition context that is also used for checkpointing etc.</param>
        /// <param name="operationDescription">The operation description that the exception occurred in</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        Task PartitionHandleErrorAsync(Exception exception, ProcessorPartitionContext partitionContext, string operationDescription, CancellationToken cancellationToken);

        /// <summary>
        /// Initialization of the partition
        /// </summary>
        /// <param name="partitionContext">The partition context that is also used for checkpointing etc.</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns></returns>
        Task PartitionInitializeAsync(ProcessorPartitionContext partitionContext, CancellationToken cancellationToken);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="partitionContext">The partition context that is also used for checkpointing etc.</param>
        /// <param name="reason">The reason for shutting down</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        Task PartitionStopAsync(ProcessorPartitionContext partitionContext, ProcessingStoppedReason reason, CancellationToken cancellationToken);
        #endregion
    }
}
