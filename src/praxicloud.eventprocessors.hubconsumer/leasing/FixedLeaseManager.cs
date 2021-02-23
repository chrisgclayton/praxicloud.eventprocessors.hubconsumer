// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.leasing
{
    #region Using Clauses
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs.Primitives;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.metrics;
    using praxicloud.core.security;
    using praxicloud.eventprocessors;
    #endregion

    /// <summary>
    /// A lease manager based on a fixed partition assignment
    /// </summary>
    public sealed class FixedLeaseManager : ILeaseManager
    {
        #region Constants
        /// <summary>
        /// The version of the lease
        /// </summary>
        private const string LeaseVersion = "1.0";

        /// <summary>
        /// A lease owner name to associated with the leases not owned by the manager instance
        /// </summary>
        private const string NotOwned = "NotOwned";
        #endregion
        #region Variables
        private static readonly DateTimeOffset _maximumDateTime = DateTimeOffset.MaxValue.AddYears(-1);
        #endregion
        #region Proeprties
        /// <summary>
        /// Controls access to the underlying lease collection
        /// </summary>
        private SemaphoreSlim _collectionControl = new SemaphoreSlim(1);

        /// <summary>
        /// The fixed partition manager used to determine ownership
        /// </summary>
        private readonly FixedPartitionManager _partitionManager;

        /// <summary>
        /// The number of managers in use
        /// </summary>
        private int _managerCount;

        /// <summary>
        /// A dictionary of leases that contain their context
        /// </summary>
        private Dictionary<string, EventProcessorPartitionOwnership> _leases = new Dictionary<string, EventProcessorPartitionOwnership>();

        /// <summary>
        /// The options that control the behavior of the processor
        /// </summary>
        private FixedProcessorClientOptions _options;

        /// <summary>
        /// The currently used client
        /// </summary>
        private FixedProcessorClient _client;

        /// <summary>
        /// A metric container used to track the number of times scale events have occurred
        /// </summary>
        private readonly ICounter _updateManagerCounter;

        /// <summary>
        /// A metric container used to track the number of times ownership claims have been perfored
        /// </summary>
        private readonly ICounter _ownershipClaimCounter;

        /// <summary>
        /// A metric container used to track the number of times the ownership has been listed
        /// </summary>
        private readonly ICounter _listOwnershipCounter;
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="metricFactory">A factory to create metric containers from</param>
        /// <param name="connectionString">The connection string to the Event Hub</param>
        /// <param name="managerId">The zero based index of the fixed partition manager</param>
        /// <param name="managerCount">The number of fixed partition managers that cooperating</param>
        public FixedLeaseManager(ILogger logger, IMetricFactory metricFactory, string connectionString, int managerId, int managerCount)
        {
            _partitionManager = new FixedPartitionManager(logger, connectionString, managerId);
            _managerCount = managerCount;

            _updateManagerCounter = metricFactory.CreateCounter("flm-update-counter", "The number of times the manager count has been updated", false, new string[0]);
            _ownershipClaimCounter = metricFactory.CreateCounter("flm-ownership-claim-counter", "The number of times a partition claim request has been executed (multiples per request)", false, new string[0]);
            _listOwnershipCounter = metricFactory.CreateCounter("flm-ownership-list-counter", "The number of times a partition claim list request has been executed", false, new string[0]);
        }
        #endregion
        #region Methods
        /// <inheritdoc />
        public async Task UpdateManagerCount(int count, CancellationToken cancellationToken)
        {
            Guard.NotLessThan(nameof(count), count, 1);

            var acquiredLock = false;

            _updateManagerCounter.Increment();

            try
            {
                await _collectionControl.WaitAsync(cancellationToken).ConfigureAwait(false);
                acquiredLock = true;

                if (_managerCount != count)
                {
                    _managerCount = count;

                    await _partitionManager.UpdateManagerQuantityAsync(count, cancellationToken).ConfigureAwait(false);
                    await _partitionManager.InitializeAsync(count).ConfigureAwait(false);
                    PopulateCollectionInternal();
                }
            }
            finally
            {
                if (acquiredLock)
                {
                    _collectionControl.Release();
                }
            }
        }

        /// <inheritdoc />
        public async Task<bool> InitializeAsync(FixedProcessorClient client, FixedProcessorClientOptions options, CancellationToken cancellationToken)
        {
            Guard.NotNull(nameof(client), client);
            Guard.NotNull(nameof(options), options);

            _options = options;
            _client = client;

            var acquiredLock = false;

            try
            {
                await _collectionControl.WaitAsync(cancellationToken).ConfigureAwait(false);
                acquiredLock = true;

                var managerCount = _managerCount;
                await _partitionManager.InitializeAsync(managerCount).ConfigureAwait(false);
                PopulateCollectionInternal();
            }
            finally
            {
                if (acquiredLock)
                {
                    _collectionControl.Release();
                }
            }

            return true;
        }


        /// <inheritdoc />
        public async Task<IEnumerable<EventProcessorPartitionOwnership>> ClaimOwnershipAsync(IEnumerable<EventProcessorPartitionOwnership> desiredOwnership, CancellationToken cancellationToken)
        {
            List<EventProcessorPartitionOwnership> results = new List<EventProcessorPartitionOwnership>();
            var acquiredLock = false;

            _ownershipClaimCounter.Increment();

            try
            {
                await _collectionControl.WaitAsync(cancellationToken).ConfigureAwait(false);
                acquiredLock = true;

                var desired = desiredOwnership.ToArray();

                results = new List<EventProcessorPartitionOwnership>(desired.Length);

                foreach (var partitionOwnership in desired)
                {
                    if (await _partitionManager.IsOwnerAsync(partitionOwnership.PartitionId, cancellationToken).ConfigureAwait(false))
                    {
                        var existingOwnership = _leases[partitionOwnership.PartitionId];

                        existingOwnership.OwnerIdentifier = _client.Identifier;
                        existingOwnership.LastModifiedTime = DateTimeOffset.UtcNow;

                        results.Add(CloneOwnership(existingOwnership));
                    }
                }
            }
            finally
            {
                if (acquiredLock)
                {
                    _collectionControl.Release();
                }
            }

            return results;
        }

        /// <inheritdoc />
        public Task<bool> CreateStoreIfNotExistsAsync(FixedProcessorClientOptions options, CancellationToken cancellationToken)
        {
            return Task.FromResult(true);
        }


        /// <inheritdoc />
        public async Task<IEnumerable<EventProcessorPartitionOwnership>> ListOwnershipAsync(CancellationToken cancellationToken)
        {
            EventProcessorPartitionOwnership[] results;
            var acquiredLock = false;

            _listOwnershipCounter.Increment();

            try
            {
                await _collectionControl.WaitAsync(cancellationToken).ConfigureAwait(false);
                acquiredLock = true;

                results = new EventProcessorPartitionOwnership[_leases.Count];
                var index = 0;

                foreach (var ownership in _leases.Values)
                {
                    results[index++] = CloneOwnership(ownership);
                }
            }
            finally
            {
                if (acquiredLock)
                {
                    _collectionControl.Release();
                }
            }

            return results;
        }

        /// <summary>
        /// An shared method for populating the lease collection
        /// </summary>
        private void PopulateCollectionInternal()
        {
            var leases = new Dictionary<string, EventProcessorPartitionOwnership>();
            var allPartitions = _partitionManager.GetPartitions();

            foreach (var partition in allPartitions)
            {
                var isOwned = _partitionManager.IsOwner(partition);
                var exists = _leases.TryGetValue(partition, out var existing);

                var ownership = new EventProcessorPartitionOwnership
                {
                    ConsumerGroup = _client.ConsumerGroup,
                    EventHubName = _client.EventHubName,
                    FullyQualifiedNamespace = _client.FullyQualifiedNamespace,
                    PartitionId = partition,
                    Version = LeaseVersion
                };

                if (isOwned)
                {
                    if (exists && string.Equals(existing.OwnerIdentifier, _client.Identifier, StringComparison.Ordinal))
                    {
                        ownership.OwnerIdentifier = existing.OwnerIdentifier;
                        ownership.LastModifiedTime = existing.LastModifiedTime;
                    }
                    else
                    {
                        ownership.OwnerIdentifier = null;
                        ownership.LastModifiedTime = DateTimeOffset.MinValue;
                    }
                }
                else
                {
                    ownership.OwnerIdentifier = NotOwned;
                    ownership.LastModifiedTime = _maximumDateTime;
                }

                leases.Add(partition, ownership);
            }

            _leases = leases;
        }

        /// <summary>
        /// Clones the ownership instance
        /// </summary>
        /// <param name="ownership">The ownership to clone</param>
        /// <returns>The deep clone of the source ownership</returns>
        private static EventProcessorPartitionOwnership CloneOwnership(EventProcessorPartitionOwnership ownership)
        {
            return new EventProcessorPartitionOwnership
            {
                ConsumerGroup = ownership.ConsumerGroup,
                EventHubName = ownership.EventHubName,
                FullyQualifiedNamespace = ownership.FullyQualifiedNamespace,
                LastModifiedTime = ownership.LastModifiedTime,
                OwnerIdentifier = ownership.OwnerIdentifier,
                PartitionId = ownership.PartitionId,
                Version = ownership.Version                
            };
        }
        #endregion
    }
}
