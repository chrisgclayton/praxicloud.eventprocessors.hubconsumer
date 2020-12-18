// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.storage
{
    #region Using Clauses
    using Azure.Core;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.metrics;
    using praxicloud.core.security;
    using praxicloud.eventprocessors.hubconsumer;
    using System;
    #endregion

    /// <summary>
    /// A blob storage based poison message monitor that counts a message as poison after the specified number of times received
    /// </summary>
    public class BlobStorageCountPoisonedMessageMonitor : BlobStoragePoisonedMonitorBase
    {
        #region Variables
        /// <summary>
        /// The number of times that a message can be received before it is considered poisoned
        /// </summary>
        private readonly int _allowedReceiveCount;
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="metricFactory">A factory to create metric containers from</param>
        /// <param name="containerName">The name of the container to write the poison message data to</param>
        /// <param name="allowedReceiveCount">The number of times that a message can be received before it is considered poison (e.g. 1 allows for a single receive)</param>
        /// <param name="connectionString">The connection string of the Azure Storage account</param>
        /// <param name="retryOptions">Rety options for the Azure Storage client, or null for default</param>
        public BlobStorageCountPoisonedMessageMonitor(ILogger logger, IMetricFactory metricFactory, string containerName, string connectionString, int allowedReceiveCount, RetryOptions retryOptions = null) : base(logger, metricFactory, containerName, connectionString, retryOptions)
        {
            Guard.NotLessThan(nameof(allowedReceiveCount), allowedReceiveCount, 1);

            _allowedReceiveCount = allowedReceiveCount;
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="metricFactory">A factory to create metric containers from</param>
        /// <param name="storageAccountUri">The Azure Storage account URI</param>
        /// <param name="containerName">The name of the container to write the poison message data to</param>
        /// <param name="allowedReceiveCount">The number of times that a message can be received before it is considered poison (e.g. 1 allows for a single receive)</param>
        /// <param name="credential">The credential provider such as managed identity provider</param>
        /// <param name="retryOptions">Rety options for the Azure Storage client, or null for default</param>
        public BlobStorageCountPoisonedMessageMonitor(ILogger logger, IMetricFactory metricFactory, Uri storageAccountUri, string containerName, TokenCredential credential, int allowedReceiveCount, RetryOptions retryOptions = null) : base(logger, metricFactory, storageAccountUri, containerName, credential, retryOptions)
        {
            Guard.NotLessThan(nameof(allowedReceiveCount), allowedReceiveCount, 1);

            _allowedReceiveCount = allowedReceiveCount;
        }
        #endregion
        #region Properties
        /// <inheritdoc />
        public override string Name => nameof(BlobStorageCountPoisonedMessageMonitor);
        #endregion
        #region Methods
        /// <inheritdoc />
        protected override bool CheckIfPoisoned(string partitionId, long sequenceNumber, PoisonData data)
        {
            var poisonDataSequenceNumber = data.SequenceNumber ?? -1;

            return poisonDataSequenceNumber >= 0 && data.ReceiveCount > _allowedReceiveCount && poisonDataSequenceNumber == sequenceNumber;
        }
        #endregion
    }
}
