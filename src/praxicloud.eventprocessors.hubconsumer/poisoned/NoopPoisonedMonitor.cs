// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.poisoned
{
    #region Using Clauses
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.metrics;
    #endregion

    /// <summary>
    /// A poison message monitor that does not 
    /// </summary>
    public sealed class NoopPoisonedMonitor : IPoisonedMonitor
    {
        #region Variables
        /// <summary>
        /// A simple poison message handling instance that is a noop
        /// </summary>
        private readonly static IPoisonedMonitor _instance = new NoopPoisonedMonitor();
        #endregion
        #region Properties
        /// <inheritdoc />
        public string Name => nameof(NoopPoisonedMonitor);

        /// <inheritdoc />
        public Task<PoisonData> GetPoisonDataAsync(string partitionId, CancellationToken cancellationToken)
        {
            return Task.FromResult(new PoisonData
            {
                PartitionId = partitionId,
                ReceiveCount = 1,
                SequenceNumber = -1
            });
        }
        #endregion
        #region Methods
        /// <inheritdoc />
        public Task<bool> CreateStoreIfNotExistsAsync(FixedProcessorClient client, FixedProcessorClientOptions options, CancellationToken cancellationToken)
        {
            return Task.FromResult(true);
        }

        /// <inheritdoc />
        public Task<bool> InitializeAsync(FixedProcessorClient client, FixedProcessorClientOptions options, ILogger logger, IMetricFactory metricFactory, CancellationToken cancellationToken)
        {
            return Task.FromResult(true);
        }

        /// <inheritdoc />
        public Task<bool> IsPoisonedMessageAsync(string partitionId, long sequenceNumber, CancellationToken cancellationToken)
        {
            return Task.FromResult(false);
        }

        /// <inheritdoc />
        public Task<bool> UpdatePoisonDataAsync(PoisonData data, CancellationToken cancellationToken)
        {
            return Task.FromResult(true);
        }

        /// <summary>
        /// A singleton instance of the monitor
        /// </summary>
        public static IPoisonedMonitor Instance => _instance;
        #endregion
    }
}
