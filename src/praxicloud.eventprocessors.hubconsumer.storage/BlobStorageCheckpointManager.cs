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
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Consumer;
    using Azure.Messaging.EventHubs.Primitives;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.metrics;
    using praxicloud.core.security;
    using praxicloud.eventprocessors.hubconsumer.checkpointing;
    #endregion

    /// <summary>
    /// A checkpoint manager that based on Azure Blob Storage
    /// </summary>
    public sealed class BlobStorageCheckpointManager : ICheckpointManager
    {
        #region Constants
        /// <summary>
        /// The metadata key of the BLOB where the sequence number is stored
        /// </summary>
        private const string SequenceNumber = "sequencenumber";

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
        /// The Azure BLOB Storage client used to interact with the checkpoint container and blobs
        /// </summary>
        private BlobContainerClient _client;

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

        /// <summary>
        /// A summmary metric container that records blob operation timing
        /// </summary>
        private ISummary _updateBlobTiming;

        /// <summary>
        /// The logger to write debugging and diagnostics information to
        /// </summary>
        private ILogger _logger;
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
        public BlobStorageCheckpointManager(ILogger logger, IMetricFactory metricFactory, string containerName, string connectionString, RetryOptions retryOptions = null)
        {
            Guard.NotNullOrWhitespace(nameof(containerName), containerName);
            Guard.NotNullOrWhitespace(nameof(connectionString), connectionString);

            var options = GetOptions(retryOptions);

            _client = new BlobContainerClient(connectionString, containerName, options);
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
        public BlobStorageCheckpointManager(ILogger logger, IMetricFactory metricFactory, Uri storageAccountUri, string containerName, TokenCredential credential, RetryOptions retryOptions = null)
        {
            Guard.NotNullOrWhitespace(nameof(containerName), containerName);
            Guard.NotNull(nameof(credential), credential);

            var options = GetOptions(retryOptions);
            var builder = new BlobUriBuilder(storageAccountUri);

            builder.BlobContainerName = containerName;
            _client = new BlobContainerClient(builder.ToUri(), credential, options);
            ConfigureInstance(logger, metricFactory);
        }
        #endregion
        #region Methods
        /// <summary>
        /// Configures the basic information associated with the instance, including metrics setup
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="metricFactory">The metrics factory used to create metrics containers from</param>
        private void ConfigureInstance(ILogger logger, IMetricFactory metricFactory)
        {
            Guard.NotNull(nameof(logger), logger);
            Guard.NotNull(nameof(metricFactory), metricFactory);

            _logger = logger;

            using (logger.BeginScope("Configuring Instance"))
            {
                _logger.LogInformation("Beginning Configuring instance");
                _listCheckpointsCounter = metricFactory.CreateCounter("bscm-list-count", "The number of times the list of checkpoints has been invoked since the start of the instance.", false, new string[0]);
                _updateCheckpointCounter = metricFactory.CreateCounter("bscm-update-count", "The number of times the update checkpoint has been invoked since the start of the instance.", false, new string[0]);
                _listBlobTiming = metricFactory.CreateSummary("bscm-list-timing", "The timing of the calls to the Azure Storage for list blob operations", 10, false, new string[0]);
                _updateBlobTiming = metricFactory.CreateSummary("bscm-update-timing", "The timing of the calls to the Azure Storage for update blob operations", 10, false, new string[0]);
                _logger.LogInformation("Finished Configuring instance");
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

        /// <summary>
        /// Initializes the instance which must be called 1 time before use
        /// </summary>
        /// <param name="client">The processor client</param>
        /// <param name="options">The client options</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>True if initialized successfully</returns>
        public Task<bool> InitializeAsync(FixedProcessorClient client, FixedProcessorClientOptions options, CancellationToken cancellationToken)
        {
            using (_logger.BeginScope("Initialize checkpoint store"))
            {
                _logger.LogDebug("Initializing checkpoint store");
                _checkpointPrefix = string.IsNullOrWhiteSpace(options.CheckpointPrefix) ? string.Format(CultureInfo.InvariantCulture, client.FullyQualifiedNamespace.ToLowerInvariant(), client.EventHubName.ToLowerInvariant(), client.ConsumerGroup.ToLowerInvariant()) :
                                                                                          string.Format(CultureInfo.InvariantCulture, options.CheckpointPrefix, client.FullyQualifiedNamespace.ToLowerInvariant(), client.EventHubName.ToLowerInvariant(), client.ConsumerGroup.ToLowerInvariant());
                _logger.LogInformation("Store prefix {location}", _checkpointPrefix);
                _blobNameFormatString = string.Concat(_checkpointPrefix, "{0}");
                _processorClient = client;
            }

            return Task.FromResult(true);
        }

        /// <summary>
        /// Initializes the checkpoint manager store
        /// </summary>
        /// <param name="options">The processor options</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>True if the store is created successflly or exists</returns>
        public async Task<bool> CreateStoreIfNotExistsAsync(FixedProcessorClientOptions options, CancellationToken cancellationToken)
        {
            var success = false;

            using (_logger.BeginScope("Creating store"))
            {
                try
                {
                    _logger.LogInformation("Creating checkpoing store");
                    await _client.CreateIfNotExistsAsync().ConfigureAwait(false);
                    success = true;
                    _logger.LogDebug("Completed store creation");
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Error creating checkpoint store");
                }
            }

            return success;
        }

        /// <summary>
        /// retrieves a list of all checkpoints known to the store
        /// </summary>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>A list of checkpoints</returns>
        public async Task<IEnumerable<EventProcessorCheckpoint>> ListCheckpointsAsync(CancellationToken cancellationToken)
        {
            var checkpoints = new List<EventProcessorCheckpoint>();

            _listCheckpointsCounter.Increment();

            using (_logger.BeginScope("Listing checkpoints"))
            {
                // Timing added around loop although not ideal. This is tied the paging async
                using (_listBlobTiming.Time())
                {
                    _logger.LogInformation("Retrieving checkpoints");

                    await foreach (BlobItem blob in _client.GetBlobsAsync(traits: BlobTraits.Metadata, prefix: _checkpointPrefix, cancellationToken: cancellationToken).ConfigureAwait(false))
                    {
                        var partitionId = blob.Name.Substring(_checkpointPrefix.Length);
                        var startingPosition = default(EventPosition?);
                        var sequenceNumber = default(long?);

                        _logger.LogDebug("Retrieved blob for partition {partitionId}", partitionId);

                        if (blob.Metadata.TryGetValue(SequenceNumber, out var str) && long.TryParse(str, NumberStyles.Integer, CultureInfo.InvariantCulture, out var result))
                        {
                            sequenceNumber = result;
                            startingPosition = EventPosition.FromSequenceNumber(result, false);
                        }

                        if (startingPosition.HasValue)
                        {
                            _logger.LogDebug("Checkpoint was found with a sequence number of {sequenceNumber}", sequenceNumber);

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
                            _logger.LogError("An invalid checkpoint was found, no starting position");
                        }
                    }
                }
            }

            return checkpoints;
        }

        /// <summary>
        /// Updates or adds the checkpoint 
        /// </summary>
        /// <param name="eventData">The event data that the checkpoint is associated with</param>
        /// <param name="context">The partition context the checkpoint is associated with</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>True if the update is successful</returns>
        public async Task<bool> UpdateCheckpointAsync(EventData eventData, PartitionContext context, CancellationToken cancellationToken)
        {
            var success = false;

            _updateCheckpointCounter.Increment();

            using (_logger.BeginScope("Updating checkpoint"))
            {
                _logger.LogInformation("Updating checkpoint for partition {partitionId} with sequence Number {sequenceNumber}", context.PartitionId, eventData.SequenceNumber);

                if (!_blobNames.TryGetValue(context.PartitionId, out var blobName))
                {
                    blobName = string.Format(_blobNameFormatString, context.PartitionId);
                    _blobNames.TryAdd(context.PartitionId, blobName);
                }

                var blobClient = _client.GetBlobClient(blobName);
                var metadata = new Dictionary<string, string>() { { SequenceNumber, eventData.SequenceNumber.ToString(CultureInfo.InvariantCulture) } };

                try
                {
                    try
                    {
                        using (_updateBlobTiming.Time())
                        {
                            await blobClient.SetMetadataAsync(metadata, cancellationToken: cancellationToken).ConfigureAwait(false);
                            _logger.LogDebug("Metadata set for partition {partitionId}", context.PartitionId);
                        }
                        success = true;
                    }
                    catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.BlobNotFound)
                    {
                        _logger.LogInformation("Blob not found partition {partitionId}", context.PartitionId);

                        using var blobContent = new MemoryStream(Array.Empty<byte>());

                        using (_updateBlobTiming.Time())
                        {
                            await blobClient.UploadAsync(blobContent, metadata: metadata, cancellationToken: cancellationToken).ConfigureAwait(false);
                            _logger.LogDebug("Blob updated for partition {partitionId}", context.PartitionId);
                        }

                        success = true;
                    }
                    catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.ContainerNotFound)
                    {
                        _logger.LogInformation("Container not found {partitionId}", context.PartitionId);

                        await _client.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                        using var blobContent = new MemoryStream(Array.Empty<byte>());

                        using (_updateBlobTiming.Time())
                        {
                            await blobClient.UploadAsync(blobContent, metadata: metadata, cancellationToken: cancellationToken).ConfigureAwait(false);
                            _logger.LogDebug("Blob uploaded for partition {partitionId}", context.PartitionId);
                        }

                        success = true;
                    }
                }
                catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.BlobNotFound)
                {
                    _logger.LogError(e, "Error ending with blob not found for partition {partitionId}", context.PartitionId);
                    throw new RequestFailedException(BlobDoesNotExist);
                }
                catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.ContainerNotFound)
                {
                    _logger.LogError(e, "Error ending with container not found for partition {partitionId}", context.PartitionId);
                    await _client.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                    throw new RequestFailedException(ContainerDoesNotExist);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Unknown error resulted for partition {partitionId}", context.PartitionId);
                    throw;
                }
            }

            return success;
        }
        #endregion
    }
}
