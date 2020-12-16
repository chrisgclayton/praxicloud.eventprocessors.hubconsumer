// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.storage
{
    #region Using Clauses
    using Azure;
    using Azure.Core;
    using Azure.Messaging.EventHubs;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.metrics;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    #endregion

    /// <summary>
    /// A checkpoint manager that uses metadata on Azure Storage Blobs to maintain checkpoint data
    /// </summary>
    public sealed class BlobStorageMetadataCheckpointManager : BlobStorageCheckpointBase
    {
        #region Constants
        /// <summary>
        /// The metadata key of the BLOB where the sequence number is stored
        /// </summary>
        private const string SequenceNumber = "sequencenumber";
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
        public BlobStorageMetadataCheckpointManager(ILogger logger, IMetricFactory metricFactory, string containerName, string connectionString, RetryOptions retryOptions = null) : base(logger, metricFactory, containerName, connectionString, retryOptions)
        {
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
        public BlobStorageMetadataCheckpointManager(ILogger logger, IMetricFactory metricFactory, Uri storageAccountUri, string containerName, TokenCredential credential, RetryOptions retryOptions = null) : base(logger, metricFactory, storageAccountUri, containerName, credential, retryOptions)
        {
        }
        #endregion
        #region Methods
        /// <inheritdoc />
        protected override Task<long?> GetCheckpointSequenceNumberAsync(string partitionId, BlobItem blob, CancellationToken cancellationToken)
        {
            long? sequenceNumber = default;

            if (blob.Metadata.TryGetValue(SequenceNumber, out var str) && long.TryParse(str, NumberStyles.Integer, CultureInfo.InvariantCulture, out var result))
            {
                sequenceNumber = result;                
            }

            return Task.FromResult(sequenceNumber);
        }

        /// <inheritdoc />
        protected override async Task<bool> SetCheckpointDataAsync(BlobClient blobClient, string partitionId, EventData eventData, CancellationToken cancellationToken)
        {
            var success = false;

            var metadata = new Dictionary<string, string>() { { SequenceNumber, eventData.SequenceNumber.ToString(CultureInfo.InvariantCulture) } };

            try
            {
                using (UpdateBlobTiming.Time())
                {
                    await blobClient.SetMetadataAsync(metadata, cancellationToken: cancellationToken).ConfigureAwait(false);
                    Logger.LogDebug("Metadata set for partition {partitionId}", partitionId);
                }

                success = true;
            }
            catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.BlobNotFound)
            {
                Logger.LogInformation("Blob not found partition {partitionId}", partitionId);

                using var blobContent = new MemoryStream(Array.Empty<byte>());

                using (UpdateBlobTiming.Time())
                {                        
                    await blobClient.UploadAsync(blobContent, metadata: metadata, cancellationToken: cancellationToken).ConfigureAwait(false);
                    Logger.LogDebug("Blob updated for partition {partitionId}", partitionId);
                }

                success = true;
            }
            catch (RequestFailedException e) when (e.ErrorCode == BlobErrorCode.ContainerNotFound)
            {
                Logger.LogInformation("Container not found {partitionId}", partitionId);

                await Client.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                using var blobContent = new MemoryStream(Array.Empty<byte>());

                using (UpdateBlobTiming.Time())
                {
                    await blobClient.UploadAsync(blobContent, metadata: metadata, cancellationToken: cancellationToken).ConfigureAwait(false);
                    Logger.LogDebug("Blob uploaded for partition {partitionId}", partitionId);
                }

                success = true;
            }

            return success;
        }
        #endregion
    }
}
