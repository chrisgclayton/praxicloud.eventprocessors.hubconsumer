// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.storage
{
    #region Using Clauses
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Core;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.metrics;
    using praxicloud.core.security;
    #endregion

    public abstract class BlobStoragePoisonedMonitorBase : IPoisonedMonitor
    {
        #region Constants
        /// <summary>
        /// An error message for errors when the BLOB does not exist
        /// </summary>
        private const string BlobDoesNotExist = "The Azure Storage Blob used by the Poison Monitor does not exist.";

        /// <summary>
        /// An error message for errors when the Container does not exist
        /// </summary>
        private const string ContainerDoesNotExist = "The Azure Storage Container used by the Poison Monitor does not exist.";

        /// <summary>
        /// The metadata key of the BLOB where the sequence number is stored
        /// </summary>
        private const string SequenceNumber = "sequencenumber";

        /// <summary>
        /// The metadata key of the BLOB where the partition is stored
        /// </summary>
        private const string PartitionId = "partitionId";

        /// <summary>
        /// The metadata key of the BLOB where the received count
        /// </summary>
        private const string ReceivedCount = "receivedCount";
        #endregion
        #region Variables
        /// <summary>
        /// The format string used for the naming of Blobs by partition
        /// </summary>
        private string _blobNameFormatString;

        /// <summary>
        /// A prefix that is used for the poison message store allowing for variations in storage locations by processors
        /// </summary>
        private string _messageStorePrefix;

        /// <summary>
        /// A metric container that counts the number of times the poison message test has been invoked
        /// </summary>
        private ICounter _testCounter;

        /// <summary>
        /// The number of times the poison message store has been updated since the start of the instance
        /// </summary>
        private ICounter _updateCounter;

        /// <summary>
        /// A summary metric container that records blob update timings
        /// </summary>
        private ISummary _updateBlobTiming;

        /// <summary>
        /// A summary metric container that records blob read timings
        /// </summary>
        private ISummary _readBlobTiming;

        /// <summary>
        /// The id of the partition being tracked
        /// </summary>
        private string _partitionId;

        /// <summary>
        /// The current poison message data
        /// </summary>
        private ConcurrentDictionary<string, PoisonData> _currentData = new ConcurrentDictionary<string, PoisonData>();
        #endregion
        #region Properties
        /// <inheritdoc />
        public abstract string Name { get; }

        /// <summary>
        /// The Azure BLOB Storage client used to interact with the checkpoint container and blobs
        /// </summary>
        protected BlobContainerClient Client { get; }

        /// <summary>
        /// The logger to write debugging and diagnostics information to
        /// </summary>
        protected ILogger Logger { get; private set; }

        /// <summary>
        /// A factory to create metric containers from
        /// </summary>
        protected IMetricFactory MetricFactory { get; private set; }

        /// <summary>
        /// The event processor client that the checkpoint manager is associated with
        /// </summary>
        protected FixedProcessorClient FixedProcessorClient { get; }
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="metricFactory">A factory to create metric containers from</param>
        /// <param name="containerName">The name of the container to write the poison message data to</param>
        /// <param name="connectionString">The connection string of the Azure Storage account</param>
        /// <param name="retryOptions">Rety options for the Azure Storage client, or null for default</param>
        protected BlobStoragePoisonedMonitorBase(ILogger logger, IMetricFactory metricFactory, string containerName, string connectionString, RetryOptions retryOptions = null)
        {
            Guard.NotNullOrWhitespace(nameof(containerName), containerName);
            Guard.NotNullOrWhitespace(nameof(connectionString), connectionString);

            var options = GetOptions(retryOptions);

            Client = new BlobContainerClient(connectionString, containerName, options);
            ConfigureInstance(logger, metricFactory);
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="metricFactory">A factory to create metric containers from</param>
        /// <param name="storageAccountUri">The Azure Storage account URI</param>
        /// <param name="containerName">The name of the container to write the poison message data to</param>
        /// <param name="credential">The credential provider such as managed identity provider</param>
        /// <param name="retryOptions">Rety options for the Azure Storage client, or null for default</param>
        protected BlobStoragePoisonedMonitorBase(ILogger logger, IMetricFactory metricFactory, Uri storageAccountUri, string containerName, TokenCredential credential, RetryOptions retryOptions = null)
        {
            Guard.NotNullOrWhitespace(nameof(containerName), containerName);
            Guard.NotNull(nameof(credential), credential);

            var options = GetOptions(retryOptions);
            var builder = new BlobUriBuilder(storageAccountUri);

            builder.BlobContainerName = containerName;
            Client = new BlobContainerClient(builder.ToUri(), credential, options);
            ConfigureInstance(logger, metricFactory);
        }
        #endregion
        #region Methods
        /// <inheritdoc />
        public async Task<bool> CreateStoreIfNotExistsAsync(FixedProcessorClient client, FixedProcessorClientOptions options, CancellationToken cancellationToken)
        {
            var success = false;

            using (Logger.BeginScope("Creating store"))
            {
                try
                {
                    Logger.LogInformation("Creating poison message store");
                    await Client.CreateIfNotExistsAsync().ConfigureAwait(false);
                    success = true;
                    Logger.LogDebug("Completed store creation");
                }
                catch (Exception e)
                {
                    Logger.LogError(e, "Error creating poison message store");
                }
            }

            return success;
        }

        /// <inheritdoc />
        public async Task<PoisonData> GetPoisonDataAsync(string partitionId, CancellationToken cancellationToken)
        {
            PoisonData data;

            if(!_currentData.TryGetValue(partitionId, out data)) data = default;

            if(data == default)
            {
                using (Logger.BeginScope("Read Poison Data"))
                {
                    Logger.LogInformation("Reading poison data");

                    var blobName = string.Format(_blobNameFormatString, data.PartitionId);
                    var blobClient = Client.GetBlobClient(blobName);

                    Response<BlobProperties> response;

                    using (_readBlobTiming.Time())
                    {
                        response = await blobClient.GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                    }

                    if (response.Value != null)
                    {
                        long? sequenceNumber = default;

                        if (response.Value.Metadata.TryGetValue(SequenceNumber, out var sequence) && long.TryParse(sequence, NumberStyles.Integer, CultureInfo.InvariantCulture, out var sequenceResult))
                        {
                            sequenceNumber = sequenceResult;
                        }

                        int? receiveCount = default;

                        if (response.Value.Metadata.TryGetValue(ReceivedCount, out var received) && int.TryParse(received, NumberStyles.Integer, CultureInfo.InvariantCulture, out var receivedResult))
                        {
                            receiveCount = receivedResult;
                        }

                        data = new PoisonData
                        {
                            PartitionId = _partitionId,
                            ReceiveCount = receiveCount ?? 0,
                            SequenceNumber = sequenceNumber ?? -1L
                        };
                    }
                }

                _currentData.TryAdd(partitionId, data);
            }

            return data;
        }


        /// <inheritdoc />
        public Task<bool> InitializeAsync(FixedProcessorClient client, FixedProcessorClientOptions options, ILogger logger, IMetricFactory metricFactory, CancellationToken cancellationToken)
        {
            using (Logger.BeginScope("Initialize poison message store"))
            {
                Logger.LogDebug("Initializing poison message store");
                // Use the same prefix as the checkpoint store if it exists to reduce user configuration requirements
                _messageStorePrefix = string.IsNullOrWhiteSpace(options.CheckpointPrefix) ? string.Format(CultureInfo.InvariantCulture, client.FullyQualifiedNamespace.ToLowerInvariant(), client.EventHubName.ToLowerInvariant(), client.ConsumerGroup.ToLowerInvariant()) :
                                                                          string.Format(CultureInfo.InvariantCulture, options.CheckpointPrefix, client.FullyQualifiedNamespace.ToLowerInvariant(), client.EventHubName.ToLowerInvariant(), client.ConsumerGroup.ToLowerInvariant());
                Logger.LogInformation("Store prefix {location}", _messageStorePrefix);
                _blobNameFormatString = string.Concat(_messageStorePrefix, "{0}");
            }

            return Task.FromResult(true);
        }

        /// <inheritdoc />
        public async Task<bool> IsPoisonedMessageAsync(string partitionId, long sequenceNumber, CancellationToken cancellationToken)
        {
            _testCounter.Increment();

            if (!_currentData.ContainsKey(partitionId))
            {
                await GetPoisonDataAsync(partitionId, cancellationToken).ConfigureAwait(false);
            }

            if(!_currentData.TryGetValue(partitionId, out var partitionData)) partitionData = default;

            return CheckIfPoisoned(partitionId, sequenceNumber, partitionData);
        }

        /// <summary>
        /// Used to determine if the current sequence number is considered poison based on the data provided
        /// </summary>
        /// <param name="partitionId">The partition id the poison check is associated with</param>
        /// <param name="sequenceNumber">The sequence number of the message</param>
        /// <param name="data">The data of the poison monitor</param>
        /// <returns>True if the message is poisoned</returns>
        protected abstract bool CheckIfPoisoned(string partitionId, long sequenceNumber, PoisonData data);

        /// <inheritdoc />
        public async Task<bool> UpdatePoisonDataAsync(PoisonData data, CancellationToken cancellationToken)
        {
            var success = false;

            _updateCounter.Increment();

            using (Logger.BeginScope("Updating poison message data"))
            {
                Logger.LogInformation("Updating poison message data for partition {partitionId} with sequence Number {sequenceNumber} and received count of {receivedCount}", data.PartitionId, data.SequenceNumber, data.ReceiveCount);
                var blobName = string.Format(_blobNameFormatString, data.PartitionId);
                var blobClient = Client.GetBlobClient(blobName);

                try
                {
                    success = await SetPoisonMessageContentsAsync(blobClient, data, cancellationToken).ConfigureAwait(false);
                }
                catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.BlobNotFound)
                {
                    Logger.LogError(e, "Error ending with blob not found for partition {partitionId}", data.PartitionId);
                    throw new RequestFailedException(BlobDoesNotExist);
                }
                catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.ContainerNotFound)
                {
                    Logger.LogError(e, "Error ending with container not found for partition {partitionId}", data.PartitionId);
                    await Client.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                    throw new RequestFailedException(ContainerDoesNotExist);
                }
                catch (Exception e)
                {
                    Logger.LogError(e, "Unknown error resulted for partition {partitionId}", data.PartitionId);
                    throw;
                }
            }

            return success;
        }

        /// <summary>
        /// Updates the metadata on the BLOB to reflect the poisoned monitoring data
        /// </summary>
        /// <param name="blobClient">The BLOB to associate the metadata with</param>
        /// <param name="data">The poisoned message tracking contents</param>
        /// <param name="cancellationToken">A token to monitor for abort and cancellation requests</param>
        /// <returns>True if successful</returns>
        private async Task<bool> SetPoisonMessageContentsAsync(BlobClient blobClient, PoisonData data, CancellationToken cancellationToken)
        {
            var success = false;

            var metadata = new Dictionary<string, string>() 
            { 
                { SequenceNumber, (data.SequenceNumber ?? -1L).ToString(CultureInfo.InvariantCulture) },
                { ReceivedCount, data.ReceiveCount.ToString(CultureInfo.InvariantCulture) },
                { PartitionId, data.PartitionId }
            };

            try
            {
                using (_updateBlobTiming.Time())
                {
                    await blobClient.SetMetadataAsync(metadata, cancellationToken: cancellationToken).ConfigureAwait(false);
                    Logger.LogDebug("Metadata set for partition {partitionId}", data.PartitionId);
                }

                success = true;
            }
            catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.BlobNotFound)
            {
                Logger.LogInformation("Blob not found partition {partitionId}", data.PartitionId);

                using var blobContent = new MemoryStream(Array.Empty<byte>());

                using (_updateBlobTiming.Time())
                {
                    await blobClient.UploadAsync(blobContent, metadata: metadata, cancellationToken: cancellationToken).ConfigureAwait(false);
                    Logger.LogDebug("Blob updated for partition {partitionId}", data.PartitionId);
                }

                success = true;
            }
            catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.ContainerNotFound)
            {
                Logger.LogInformation("Container not found {partitionId}", data.PartitionId);

                await Client.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                using var blobContent = new MemoryStream(Array.Empty<byte>());

                using (_updateBlobTiming.Time())
                {
                    await blobClient.UploadAsync(blobContent, metadata: metadata, cancellationToken: cancellationToken).ConfigureAwait(false);
                    Logger.LogDebug("Blob uploaded for partition {partitionId}", data.PartitionId);
                }

                success = true;
            }

            return success;
        }

        /// <summary>
        /// Configures the basic information associated with the instance, including metrics setup
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="metricFactory">The metrics factory used to create metrics containers from</param>
        private void ConfigureInstance(ILogger logger, IMetricFactory metricFactory)
        {
            Guard.NotNull(nameof(logger), logger);
            Guard.NotNull(nameof(metricFactory), metricFactory);

            Logger = logger;
            MetricFactory = metricFactory;

            using (logger.BeginScope("Configuring Instance"))
            {
                Logger.LogInformation("Beginning Configuring instance");
                _testCounter = metricFactory.CreateCounter("bpm-test-count", "The number of times the poisoned message test has beene executed since the start of the instance.", false, new string[0]);
                _updateCounter = metricFactory.CreateCounter("bpm-update-count", "The number of times the poisoned message store has been updated since the start of the instance.", false, new string[0]);
                _updateBlobTiming = metricFactory.CreateSummary("bspm-update-timing", "The timing of the calls to the Azure Storage for updating the poisoned message values", 10, false, new string[0]);
                _readBlobTiming = metricFactory.CreateSummary("bspm-read-timing", "The timing of the calls to the Azure Storage for reading the poisoned message values", 10, false, new string[0]);
                Logger.LogInformation("Finished Configuring instance");
            }
        }

        /// <summary>
        /// Gets the Blob Client options
        /// </summary>
        /// <param name="retryOptions">Retry configuration</param>
        /// <returns>An instance of the Blob Client Options populated with default values and provided retry options</returns>
        private BlobClientOptions GetOptions(RetryOptions retryOptions)
        {
            BlobClientOptions options = null;

            if (retryOptions != null)
            {
                options = new BlobClientOptions();

                options.Retry.Mode = retryOptions.Mode;
                options.Retry.MaxRetries = retryOptions.MaxRetries;
                options.Retry.Delay = retryOptions.Delay;
                options.Retry.MaxDelay = retryOptions.MaxDelay;
                options.Retry.NetworkTimeout = retryOptions.NetworkTimeout;
            }

            return options;
        }


        #endregion
    }
}
