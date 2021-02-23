// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.storage
{
    #region Using Clauses
    using Azure;
    using Azure.Core;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Consumer;
    using Azure.Messaging.EventHubs.Primitives;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.metrics;
    using praxicloud.core.security;
    using praxicloud.eventprocessors.hubconsumer.checkpointing;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    #endregion

    /// <summary>
    /// A base checkpoint type used to store checkpoint information in Azure Blob Storage
    /// </summary>
    public abstract class BlobStorageCheckpointBase : ICheckpointManager
    {
        #region Constants
        /// <summary>
        /// An error message for errors when the BLOB does not exist
        /// </summary>
        private const string BlobDoesNotExist = "The Azure Storage Blob used by the Event Processor Client does not exist.";

        /// <summary>
        /// An error message for errors when the Container does not exist
        /// </summary>
        private const string ContainerDoesNotExist = "The Azure Storage Container used by the Event Processor Client does not exist.";
        #endregion
        #region Variables
        /// <summary>
        /// A list of BLOB names that is stored to reduce the memory impact of string creation
        /// </summary>
        private readonly ConcurrentDictionary<string, string> _blobNames = new ConcurrentDictionary<string, string>();

        /// <summary>
        /// The format string used for the naming of Blobs by partition
        /// </summary>
        private string _blobNameFormatString;

        /// <summary>
        /// A prefix that is used for the checkpoint store allowing for variations in storage locations by processors
        /// </summary>
        private string _checkpointPrefix;

        /// <summary>
        /// The event processor client that the checkpoint manager is associated with
        /// </summary>
        private FixedProcessorClient _processorClient;

        /// <summary>
        /// A metric container that counts the number of times the list checkpoints is called
        /// </summary>
        private ICounter _listCheckpointsCounter;

        /// <summary>
        /// A summary metric container that records blob list timings
        /// </summary>
        private ISummary _listBlobTiming;

        /// <summary>
        /// Counts the number of times that the update operation has been triggered
        /// </summary>
        private ICounter _updateCheckpointCounter;
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="metricFactory">A factory to create metric containers from</param>
        /// <param name="containerName">The name of the container to write the checkpoints to</param>
        /// <param name="connectionString">The connection string of the Azure Storage account</param>
        /// <param name="retryOptions">Rety options for the Azure Storage client, or null for default</param>
        protected BlobStorageCheckpointBase(ILogger logger, IMetricFactory metricFactory, string containerName, string connectionString, RetryOptions retryOptions = null)
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
        /// <param name="containerName">The name of the container to write the checkpoints to</param>
        /// <param name="credential">The credential provider such as managed identity provider</param>
        /// <param name="retryOptions">Rety options for the Azure Storage client, or null for default</param>
        protected BlobStorageCheckpointBase(ILogger logger, IMetricFactory metricFactory, Uri storageAccountUri, string containerName, TokenCredential credential, RetryOptions retryOptions = null)
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
        #region Properties
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

        /// <summary>
        /// A summmary metric container that records blob operation timing
        /// </summary>
        protected ISummary UpdateBlobTiming { get; private set; }
        #endregion
        #region Methods
        /// <inheritdoc />
        public virtual async Task<bool> CreateStoreIfNotExistsAsync(FixedProcessorClientOptions options, CancellationToken cancellationToken)
        {
            var success = false;

            using (Logger.BeginScope("Creating store"))
            {
                try
                {
                    Logger.LogInformation("Creating checkpoint store");
                    await Client.CreateIfNotExistsAsync().ConfigureAwait(false);
                    success = true;
                    Logger.LogDebug("Completed store creation");
                }
                catch (Exception e)
                {
                    Logger.LogError(e, "Error creating checkpoint store");
                }
            }

            return success;
        }

        /// <inheritdoc />
        public virtual async Task<bool> InitializeAsync(FixedProcessorClient client, FixedProcessorClientOptions options, CancellationToken cancellationToken)
        {
            var success = false;

            using (Logger.BeginScope("Initialize checkpoint store"))
            {
                Logger.LogDebug("Initializing checkpoint store");

                _checkpointPrefix = string.IsNullOrWhiteSpace(options.CheckpointPrefix) ? string.Format(CultureInfo.InvariantCulture, "{0}/{1}/{2}", client.FullyQualifiedNamespace.ToLowerInvariant(), client.EventHubName.ToLowerInvariant(), client.ConsumerGroup.Replace("$", "").ToLowerInvariant()) :
                                                                                          string.Format(CultureInfo.InvariantCulture, "{0}/{1}/{2}/{3}", options.CheckpointPrefix, client.FullyQualifiedNamespace.ToLowerInvariant(), client.EventHubName.ToLowerInvariant(), client.ConsumerGroup.Replace("$", "").ToLowerInvariant());
                Logger.LogInformation("Store prefix {location}", _checkpointPrefix);
                _blobNameFormatString = string.Concat(_checkpointPrefix, "/{0}");
                _processorClient = client;

                success = await CreateStoreIfNotExistsAsync(options, cancellationToken).ConfigureAwait(false);
            }

            return success;
        }

        /// <summary>
        /// Retrieves the sequence number from the blob
        /// </summary>
        /// <param name="blob">The Azure Storage BLOB that contains the checkpoint information</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <param name="partitionId">The partition id the checkpoint is associated with</param>
        /// <returns>The sequence number found in the BLOB or null if the BLOB is corrupt or sequence not found</returns>
        protected abstract Task<long?> GetCheckpointSequenceNumberAsync(string partitionId, BlobItem blob, CancellationToken cancellationToken);

        /// <summary>
        /// Sets the checkpoint information into the BLOB
        /// </summary>
        /// <param name="blobClient">A client used to interact with the BLOB</param>
        /// <param name="partitionId">The id of the partition the checkpoint is associated with</param>
        /// <param name="eventData">The event data to checkpoint to</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>True if the checkpoing is saved successfully</returns>
        protected abstract Task<bool> SetCheckpointDataAsync(BlobClient blobClient, string partitionId, EventData eventData, CancellationToken cancellationToken);

        /// <inheritdoc />
        public virtual async Task<IEnumerable<EventProcessorCheckpoint>> ListCheckpointsAsync(CancellationToken cancellationToken)
        {
            var checkpoints = new List<EventProcessorCheckpoint>();

            _listCheckpointsCounter.Increment();

            using (Logger.BeginScope("Listing checkpoints"))
            {
                // Timing added around loop although not ideal. This is tied the paging async
                using (_listBlobTiming.Time())
                {
                    Logger.LogInformation("Retrieving checkpoints");

                    await foreach (BlobItem blob in Client.GetBlobsAsync(traits: BlobTraits.Metadata, prefix: _checkpointPrefix, cancellationToken: cancellationToken).ConfigureAwait(false))
                    {
                        var partitionId = blob.Name.Substring(_checkpointPrefix.Length + 1);
                        var startingPosition = default(EventPosition?);

                        Logger.LogDebug("Retrieved blob for partition {partitionId}", partitionId);
                        var sequenceNumber = await GetCheckpointSequenceNumberAsync(partitionId, blob, cancellationToken).ConfigureAwait(false);

                        if(sequenceNumber.HasValue)
                        {
                            Logger.LogDebug("Checkpoint was found with a sequence number of {sequenceNumber}", sequenceNumber);

                            startingPosition = EventPosition.FromSequenceNumber(sequenceNumber.Value, false);

                            checkpoints.Add(new Checkpoint
                            {
                                FullyQualifiedNamespace = _processorClient.FullyQualifiedNamespace,
                                EventHubName = _processorClient.EventHubName,
                                ConsumerGroup = _processorClient.ConsumerGroup,
                                PartitionId = partitionId,
                                StartingPosition = startingPosition.Value,
                                SequenceNumber = sequenceNumber
                            });
                        }
                        else
                        {
                            Logger.LogError("An invalid checkpoint was found, no starting position");
                        }
                    }
                }
            }

            return checkpoints;
        }

        /// <inheritdoc />
        public virtual async Task<bool> UpdateCheckpointAsync(EventData eventData, PartitionContext context, CancellationToken cancellationToken)
        {
            var success = false;

            _updateCheckpointCounter.Increment();

            using (Logger.BeginScope("Updating checkpoint"))
            {
                Logger.LogInformation("Updating checkpoint for partition {partitionId} with sequence Number {sequenceNumber}", context.PartitionId, eventData.SequenceNumber);

                if (!_blobNames.TryGetValue(context.PartitionId, out var blobName))
                {
                    blobName = string.Format(_blobNameFormatString, context.PartitionId);
                    _blobNames.TryAdd(context.PartitionId, blobName);
                }

                var blobClient = Client.GetBlobClient(blobName);

                try
                {
                    success = await SetCheckpointDataAsync(blobClient, context.PartitionId, eventData, cancellationToken).ConfigureAwait(false);
                }
                catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.BlobNotFound)
                {
                    Logger.LogError(e, "Error ending with blob not found for partition {partitionId}", context.PartitionId);
                    throw new RequestFailedException(BlobDoesNotExist);
                }
                catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.ContainerNotFound)
                {
                    Logger.LogError(e, "Error ending with container not found for partition {partitionId}", context.PartitionId);
                    await Client.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                    throw new RequestFailedException(ContainerDoesNotExist);
                }
                catch (Exception e)
                {
                    Logger.LogError(e, "Unknown error resulted for partition {partitionId}", context.PartitionId);
                    throw;
                }
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
                _listCheckpointsCounter = metricFactory.CreateCounter("bscm-list-count", "The number of times the list of checkpoints has been invoked since the start of the instance.", false, new string[0]);
                _updateCheckpointCounter = metricFactory.CreateCounter("bscm-update-count", "The number of times the update checkpoint has been invoked since the start of the instance.", false, new string[0]);
                _listBlobTiming = metricFactory.CreateSummary("bscm-list-timing", "The timing of the calls to the Azure Storage for list blob operations", 10, false, new string[0]);
                UpdateBlobTiming = metricFactory.CreateSummary("bscm-update-timing", "The timing of the calls to the Azure Storage for update blob operations", 10, false, new string[0]);
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
