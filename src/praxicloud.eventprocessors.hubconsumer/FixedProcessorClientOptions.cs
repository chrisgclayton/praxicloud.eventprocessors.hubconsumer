// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer
{
    #region Using Clauses
    using System;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Consumer;
    using Azure.Messaging.EventHubs.Primitives;
    using Azure.Messaging.EventHubs.Processor;
    #endregion

    /// <summary>
    /// Options that define the behavior of the fixed processor client
    /// </summary>
    public class FixedProcessorClientOptions 
    {
        #region Variables
        /// <summary>
        /// The default event processor retry options if none are specified
        /// </summary>
        private static readonly EventHubsRetryOptions _defaultProcessorRetryOptions = new EventHubsRetryOptions { Delay = TimeSpan.FromMilliseconds(2000), MaximumDelay = TimeSpan.FromMilliseconds(10000), MaximumRetries = 2, Mode = EventHubsRetryMode.Exponential };

        /// <summary>
        /// The connection options default to AMQP for performance reasons
        /// </summary>
        private static readonly EventHubConnectionOptions _defaultProcessorConnectionOptions = new EventHubConnectionOptions { TransportType = EventHubsTransportType.AmqpTcp };
        #endregion
        #region Properties
        /// <summary>
        /// The identifier used to represent the event processor host. If not provided a GUID will be assigned
        /// </summary>
        public string Identifier { get; set; }

        /// <summary>
        /// The number of events that will be requested from the event hub in advance of them being required
        /// </summary>
        public int? PrefetchCount { get; set; }

        /// <summary>
        /// The number of messages to pass to the processing method at one time.
        /// </summary>
        public int? BatchSize { get; set; }

        /// <summary>
        /// The minimum time to wait between attempting to acquire and or validate the lease 
        /// </summary>
        public TimeSpan LeaseRenewalInterval { get; set; }

        /// <summary>
        /// The minimum time a lease is valid for, must be larger than the LeaseRenewalInterval
        /// </summary>
        public TimeSpan LeaseDuration { get; set; }

        /// <summary>
        /// The maximum time to wait to receive an event before triggering an empty batch of messages
        /// </summary>
        public TimeSpan ReceiveTimeout { get; set; }

        /// <summary>
        /// The retry policy for use when accessing the hub
        /// </summary>
        public EventHubsRetryOptions RetryOptions { get; set; }

        /// <summary>
        /// Connection options for use when communicating with the Event Hub
        /// </summary>
        public EventHubConnectionOptions ConnectionOptions { get; set; }

        /// <summary>
        /// The position to start processing the stream at if the checkpoint is not found
        /// </summary>
        public EventPosition StartingPosition { get; set; } = EventPosition.Earliest;

        /// <summary>
        /// Indicates if the processor should request information about the last event enqueued
        /// </summary>
        public bool TrackLastEnqueuedEventProperties { get; set; }

        /// <summary>
        /// A prefix for the checkpointing store that will be interpreted as appropriate by the final store, null for no prefix used (e.g. a file store may use it for a directory or storage as a container)
        /// </summary>
        public string CheckpointPrefix { get; set; }
        #endregion
        #region Methods
        /// <summary>
        /// Performs a deep clone
        /// </summary>
        /// <returns>An initialized instance of the options based on the contents of the current instance.</returns>
        public FixedProcessorClientOptions Clone()
        {
            return new FixedProcessorClientOptions
            {
                BatchSize = BatchSize,
                RetryOptions = RetryOptions,
                ConnectionOptions = ConnectionOptions,
                CheckpointPrefix = CheckpointPrefix,
                Identifier = Identifier,
                LeaseDuration = LeaseDuration,
                LeaseRenewalInterval = LeaseRenewalInterval,
                PrefetchCount = PrefetchCount,
                ReceiveTimeout = ReceiveTimeout,
                StartingPosition = StartingPosition,
                TrackLastEnqueuedEventProperties = TrackLastEnqueuedEventProperties
            };
        }

        /// <summary>
        /// Converts the fixed processor options to event processor options
        /// </summary>
        /// <param name="options"></param>
        public static implicit operator EventProcessorOptions(FixedProcessorClientOptions options)
        {
            var leaseRenewalInterval = TimeSpan.FromMilliseconds(Math.Max(options.LeaseRenewalInterval.TotalMilliseconds, 1000.0));
            var leaseDuration = TimeSpan.FromMilliseconds(Math.Max(options.LeaseDuration.TotalMilliseconds, 1000.0));

            if (leaseDuration.TotalMilliseconds < leaseRenewalInterval.TotalMilliseconds * 2) leaseDuration = TimeSpan.FromMilliseconds(leaseRenewalInterval.TotalMilliseconds * 2);

            return new EventProcessorOptions
            {
                RetryOptions = options.RetryOptions ?? _defaultProcessorRetryOptions,
                ConnectionOptions = options.ConnectionOptions ?? _defaultProcessorConnectionOptions,
                DefaultStartingPosition = options.StartingPosition,
                LoadBalancingStrategy = LoadBalancingStrategy.Greedy,
                PrefetchCount = Math.Max((options.PrefetchCount ?? 0), 0),
                Identifier = options.Identifier,
                LoadBalancingUpdateInterval = leaseRenewalInterval,
                MaximumWaitTime = TimeSpan.FromMilliseconds(Math.Max(options.ReceiveTimeout.TotalMilliseconds, 100.0)),
                PartitionOwnershipExpirationInterval = leaseDuration,
                TrackLastEnqueuedEventProperties = options.TrackLastEnqueuedEventProperties                
            };
        }
        #endregion
    }
}
