// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer
{
    #region using Clauses
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.ExceptionServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Core;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Primitives;
    using Azure.Messaging.EventHubs.Processor;
    using Microsoft.Extensions.Logging;
    using praxicloud.core.metrics;
    using praxicloud.core.security;
    #endregion

    /// <summary>
    /// An event processor for IoT Hub that leverages fixed partitions for leader election
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class FixedBatchProcessorClient<T> : FixedProcessorClient where T : IEventBatchProcessor
    {
        #region Variables
        /// <summary>
        /// A factory for creating processors
        /// </summary>
        private readonly ProcessorFactory _processorFactory;

        /// <summary>
        /// A logger used to record debugging and diagnostics information
        /// </summary>
        private readonly ILogger _logger;

        /// <summary>
        /// Per partition processing tasks
        /// </summary>
        private readonly ConcurrentDictionary<string, Task>_processingTasks = new ConcurrentDictionary<string, Task>();

        
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="checkpointManager">The manager responsible for checkpointing</param>
        /// <param name="leaseManager">The manager responsible for leasing</param>
        /// <param name="logger">The logger used to write debugging and diagnostics information</param>
        /// <param name="metricFactory">A factory used to create metrics</param>
        /// <param name="connectionString">The connection string to use for connecting to the Event Hubs namespace; it is expected that the Event Hub name and the shared key properties are contained in this connection string.</param>
        /// <param name="consumerGroupName">The name of the consumer group the processor is associated with. Events are read in the context of this group.</param>
        /// <param name="options">The set of options to use for the processor.</param>
        /// <param name="processorFactory">A factory used to create processors, null for default</param>
        public FixedBatchProcessorClient(ILogger logger, IMetricFactory metricFactory, string connectionString, string consumerGroupName, FixedProcessorClientOptions options, ILeaseManager leaseManager, ICheckpointManager checkpointManager, ProcessorFactory processorFactory = null) : base(logger, metricFactory, connectionString, consumerGroupName, options, leaseManager, checkpointManager)
        {
            _logger = logger;
            _processorFactory = processorFactory ?? new ProcessorFactory((ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext partitionContext) => (IProcessor)Activator.CreateInstance(typeof(T)));
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="connectionString">The connection string to use for connecting to the Event Hubs namespace; it is expected that the shared key property is contained in this connection string, but not the Event Hub name.</param>
        /// <param name="checkpointManager">The manager responsible for checkpointing</param>
        /// <param name="leaseManager">The manager responsible for leasing</param>
        /// <param name="logger">The logger used to write debugging and diagnostics information</param>
        /// <param name="metricFactory">A factory used to create metrics</param>
        /// <param name="eventHubName">The name of the specific Event Hub to associate the processor with.</param>
        /// <param name="consumerGroupName">The name of the consumer group the processor is associated with. Events are read in the context of this group.</param>
        /// <param name="options">The set of options to use for the processor.</param>
        /// <param name="processorFactory">A factory used to create processors, null for default</param>
        public FixedBatchProcessorClient(ILogger logger, IMetricFactory metricFactory, string connectionString, string eventHubName, string consumerGroupName, FixedProcessorClientOptions options, ILeaseManager leaseManager, ICheckpointManager checkpointManager, ProcessorFactory processorFactory = null) : base(logger, metricFactory, connectionString, eventHubName, consumerGroupName, options, leaseManager, checkpointManager)
        {
            _logger = logger;
            _processorFactory = processorFactory ?? new ProcessorFactory((ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext partitionContext) => (IProcessor)Activator.CreateInstance(typeof(T)));
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="credential">The Azure managed identity credential to use for authorization. Access controls may be specified by the Event Hubs namespace or the requested Event Hub, depending on Azure configuration.</param>
        /// <param name="checkpointManager">The manager responsible for checkpointing</param>
        /// <param name="leaseManager">The manager responsible for leasing</param>
        /// <param name="logger">The logger used to write debugging and diagnostics information</param>
        /// <param name="metricFactory">A factory used to create metrics</param>
        /// <param name="fullyQualifiedNamespace">The fully qualified Event Hubs namespace to connect to. This is likely to be similar to {yournamespace}.servicebus.windows.net.</param>
        /// <param name="eventHubName">The name of the specific Event Hub to associate the processor with.</param>
        /// <param name="consumerGroupName">The name of the consumer group the processor is associated with. Events are read in the context of this group.</param>
        /// <param name="options">The set of options to use for the processor.</param>
        /// <param name="processorFactory">A factory used to create processors, null for default</param>
        public FixedBatchProcessorClient(ILogger logger, IMetricFactory metricFactory, TokenCredential credential, string fullyQualifiedNamespace, string eventHubName, string consumerGroupName, FixedProcessorClientOptions options, ILeaseManager leaseManager, ICheckpointManager checkpointManager, ProcessorFactory processorFactory = null) : base(logger, metricFactory, credential, fullyQualifiedNamespace, eventHubName, consumerGroupName, options, leaseManager, checkpointManager)
        {
            _logger = logger;
            _processorFactory = processorFactory ?? new ProcessorFactory((ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext partitionContext) => (IProcessor)Activator.CreateInstance(typeof(T)));
        }

        /// <inheritdoc />
        protected override IProcessor CreateProcessor(ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext partitionContext)
        {
            return _processorFactory(logger, metricFactory, partitionContext);
        }

        /// <inheritdoc />
        protected sealed override async Task ProcessBatchAsync(IEnumerable<EventData> events, IProcessor processor, ProcessorPartitionContext partitionContext, CancellationToken cancellationToken)
        {
            var processorInstance = processor as IEventBatchProcessor;

            try
            {
                await processorInstance.PartitionProcessAsync(events, partitionContext, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e) when (!(e is TaskCanceledException))
            {
                _logger.LogError(e, "An error caught in the batch that was not a cancellation exception");
                throw;
            }
            finally
            {
                _logger.LogDebug("Completed batch processing");
            }
        }
        #endregion
    }

    public sealed class FixedEventProcessorClient<T> : FixedProcessorClient where T : IEventProcessor
    {
        #region Variables
        /// <summary>
        /// A factory for creating processors
        /// </summary>
        private readonly ProcessorFactory _processorFactory;

        /// <summary>
        /// A logger used to record debugging and diagnostics information
        /// </summary>
        private readonly ILogger _logger;
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="checkpointManager">The manager responsible for checkpointing</param>
        /// <param name="leaseManager">The manager responsible for leasing</param>
        /// <param name="logger">The logger used to write debugging and diagnostics information</param>
        /// <param name="metricFactory">A factory used to create metrics</param>
        /// <param name="connectionString">The connection string to use for connecting to the Event Hubs namespace; it is expected that the Event Hub name and the shared key properties are contained in this connection string.</param>
        /// <param name="consumerGroupName">The name of the consumer group the processor is associated with. Events are read in the context of this group.</param>
        /// <param name="options">The set of options to use for the processor.</param>
        /// <param name="processorFactory">A factory used to create processors, null for default</param>
        public FixedEventProcessorClient(ILogger logger, IMetricFactory metricFactory, string connectionString, string consumerGroupName, FixedProcessorClientOptions options, ILeaseManager leaseManager, ICheckpointManager checkpointManager, ProcessorFactory processorFactory = null) : base(logger, metricFactory, connectionString, consumerGroupName, options, leaseManager, checkpointManager)
        {
            _logger = logger;
            _processorFactory = processorFactory ?? new ProcessorFactory((ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext partitionContext) => (IProcessor)Activator.CreateInstance(typeof(T)));
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="connectionString">The connection string to use for connecting to the Event Hubs namespace; it is expected that the shared key property is contained in this connection string, but not the Event Hub name.</param>
        /// <param name="checkpointManager">The manager responsible for checkpointing</param>
        /// <param name="leaseManager">The manager responsible for leasing</param>
        /// <param name="logger">The logger used to write debugging and diagnostics information</param>
        /// <param name="metricFactory">A factory used to create metrics</param>
        /// <param name="eventHubName">The name of the specific Event Hub to associate the processor with.</param>
        /// <param name="consumerGroupName">The name of the consumer group the processor is associated with. Events are read in the context of this group.</param>
        /// <param name="options">The set of options to use for the processor.</param>
        /// <param name="processorFactory">A factory used to create processors, null for default</param>
        public FixedEventProcessorClient(ILogger logger, IMetricFactory metricFactory, string connectionString, string eventHubName, string consumerGroupName, FixedProcessorClientOptions options, ILeaseManager leaseManager, ICheckpointManager checkpointManager, ProcessorFactory processorFactory = null) : base(logger, metricFactory, connectionString, eventHubName, consumerGroupName, options, leaseManager, checkpointManager)
        {
            _logger = logger;
            _processorFactory = processorFactory ?? new ProcessorFactory((ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext partitionContext) => (IProcessor)Activator.CreateInstance(typeof(T)));
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="credential">The Azure managed identity credential to use for authorization. Access controls may be specified by the Event Hubs namespace or the requested Event Hub, depending on Azure configuration.</param>
        /// <param name="checkpointManager">The manager responsible for checkpointing</param>
        /// <param name="leaseManager">The manager responsible for leasing</param>
        /// <param name="logger">The logger used to write debugging and diagnostics information</param>
        /// <param name="metricFactory">A factory used to create metrics</param>
        /// <param name="fullyQualifiedNamespace">The fully qualified Event Hubs namespace to connect to. This is likely to be similar to {yournamespace}.servicebus.windows.net.</param>
        /// <param name="eventHubName">The name of the specific Event Hub to associate the processor with.</param>
        /// <param name="consumerGroupName">The name of the consumer group the processor is associated with. Events are read in the context of this group.</param>
        /// <param name="options">The set of options to use for the processor.</param>
        /// <param name="processorFactory">A factory used to create processors, null for default</param>
        public FixedEventProcessorClient(ILogger logger, IMetricFactory metricFactory, TokenCredential credential, string fullyQualifiedNamespace, string eventHubName, string consumerGroupName, FixedProcessorClientOptions options, ILeaseManager leaseManager, ICheckpointManager checkpointManager, ProcessorFactory processorFactory = null) : base(logger, metricFactory, credential, fullyQualifiedNamespace, eventHubName, consumerGroupName, options, leaseManager, checkpointManager)
        {
            _logger = logger;
            _processorFactory = processorFactory ?? new ProcessorFactory((ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext partitionContext) => (IProcessor)Activator.CreateInstance(typeof(T)));
        }

        /// <inheritdoc />
        protected sealed override IProcessor CreateProcessor(ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext partitionContext)
        {
            return _processorFactory(logger, metricFactory, partitionContext);
        }

        /// <inheritdoc />
        protected sealed override async Task ProcessBatchAsync(IEnumerable<EventData> events, IProcessor processor, ProcessorPartitionContext partitionContext, CancellationToken cancellationToken)
        {
            Dictionary<long, Exception> exceptions = null;
            var eventFound = false;
            var processorInstance = processor as IEventProcessor;

            try
            {
                if (events == null)
                {
                    foreach (var eventData in events)
                    {
                        eventFound = true;

                        try
                        {
                            await processorInstance.PartitionProcessAsync(eventData, partitionContext, cancellationToken).ConfigureAwait(false);
                        }
                        catch (Exception e) when (!(e is TaskCanceledException))
                        {
                            exceptions ??= new Dictionary<long, Exception>();
                            exceptions.Add(eventData.SequenceNumber, e);
                        }
                    }
                }

                if (!eventFound)
                {
                    try
                    {
                        await processorInstance.PartitionProcessAsync(null, partitionContext, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception e) when (!(e is TaskCanceledException))
                    {
                        exceptions ??= new Dictionary<long, Exception>();
                        exceptions.Add(-1, e);
                    }
                }
            }
            catch(Exception e) when (!(e is TaskCanceledException))
            {
                _logger.LogError(e, "An error caught in the batch that was not a cancellation exception");
                throw;
            }
            finally
            {
                _logger.LogDebug("Completed batch processing");
            }

            var exceptionCount = (exceptions?.Count ?? 0);

            if (exceptionCount > 0)
            {
                if(exceptionCount == 1)
                {
                    ExceptionDispatchInfo.Capture(exceptions[0]).Throw();
                }

                throw new HubProcessingException($"Error processing messages in batch on partition {partitionContext.PartitionId}", exceptions);
            }
        }
        #endregion
    }

    public abstract class FixedProcessorClient : EventProcessor<EventProcessorPartition>, IDisposable
    {
        #region Delegates
        /// <summary>
        /// Defines a factory for processors
        /// </summary>
        /// <param name="logger">The logger to write debugging and diagnostics information to</param>
        /// <param name="metricFactory">A factory to create metric containers from</param>
        /// <param name="partitionContext">The partition context the processor will be associated wiht</param>
        /// <returns>The instantiated processor</returns>
        public delegate IProcessor ProcessorFactory(ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext partitionContext);
        #endregion
        #region Using Clauses
        /// <summary>
        /// A control to manage access to the lifecycle related events
        /// </summary>
        private readonly SemaphoreSlim _lifecycleControl = new SemaphoreSlim(1);

        /// <summary>
        /// Tracks the number of times the object has been disposed of
        /// </summary>
        private int _disposalCount;
        #endregion
        #region Variables
        /// <summary>
        /// The number of times the processor has been initialized
        /// </summary>
        private int _initializationCount;                

        /// <summary>
        /// A lease manager to control ownership of partitions
        /// </summary>
        private ILeaseManager _leaseManager;

        /// <summary>
        /// A checkpoint manager to control access to and record checkpoints
        /// </summary>
        private ICheckpointManager _checkpointManager;

        /// <summary>
        /// The fixed processor options
        /// </summary>
        private FixedProcessorClientOptions _options;

        /// <summary>
        /// The logger to write debugging and diagnostics information to
        /// </summary>
        private ILogger _logger;

        /// <summary>
        /// Counts the number of errors
        /// </summary>
        private ICounter _errorCounter;

        /// <summary>
        /// The number of times that the partition processor was stopped
        /// </summary>
        private ICounter _partitionStopProcessing;

        /// <summary>
        /// The number of times the partitions have been initialized
        /// </summary>
        private ICounter _partitionInitializeProcessing;

        /// <summary>
        /// The metric factory used to create metric containers from
        /// </summary>
        private IMetricFactory _metricFactory;

        /// <summary>
        /// The partition contexts in use
        /// </summary>
        private Dictionary<string, ProcessorPartitionContext> _partitionContexts = new Dictionary<string, ProcessorPartitionContext>();

        /// <summary>
        /// The processors associated with the partitions
        /// </summary>
        private Dictionary<string, IProcessor> _partitionProcessors = new Dictionary<string, IProcessor>();
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="checkpointManager">The manager responsible for checkpointing</param>
        /// <param name="leaseManager">The manager responsible for leasing</param>
        /// <param name="logger">The logger used to write debugging and diagnostics information</param>
        /// <param name="metricFactory">A factory used to create metrics</param>
        /// <param name="connectionString">The connection string to use for connecting to the Event Hubs namespace; it is expected that the Event Hub name and the shared key properties are contained in this connection string.</param>
        /// <param name="consumerGroupName">The name of the consumer group the processor is associated with. Events are read in the context of this group.</param>
        /// <param name="options">The set of options to use for the processor.</param>
        protected FixedProcessorClient(ILogger logger, IMetricFactory metricFactory, string connectionString, string consumerGroupName, FixedProcessorClientOptions options, ILeaseManager leaseManager, ICheckpointManager checkpointManager) : base(options?.BatchSize ?? 1, consumerGroupName, connectionString, options)
        {
            ConfigureInstance(logger, metricFactory, options, leaseManager, checkpointManager);
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="connectionString">The connection string to use for connecting to the Event Hubs namespace; it is expected that the shared key property is contained in this connection string, but not the Event Hub name.</param>
        /// <param name="checkpointManager">The manager responsible for checkpointing</param>
        /// <param name="leaseManager">The manager responsible for leasing</param>
        /// <param name="logger">The logger used to write debugging and diagnostics information</param>
        /// <param name="metricFactory">A factory used to create metrics</param>
        /// <param name="eventHubName">The name of the specific Event Hub to associate the processor with.</param>
        /// <param name="consumerGroupName">The name of the consumer group the processor is associated with. Events are read in the context of this group.</param>
        /// <param name="options">The set of options to use for the processor.</param>
        protected FixedProcessorClient(ILogger logger, IMetricFactory metricFactory, string connectionString, string eventHubName, string consumerGroupName, FixedProcessorClientOptions options, ILeaseManager leaseManager, ICheckpointManager checkpointManager) : base(options?.BatchSize ?? 1, consumerGroupName, connectionString, eventHubName, options)
        {
            ConfigureInstance(logger, metricFactory, options, leaseManager, checkpointManager);
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="credential">The Azure managed identity credential to use for authorization. Access controls may be specified by the Event Hubs namespace or the requested Event Hub, depending on Azure configuration.</param>
        /// <param name="checkpointManager">The manager responsible for checkpointing</param>
        /// <param name="leaseManager">The manager responsible for leasing</param>
        /// <param name="logger">The logger used to write debugging and diagnostics information</param>
        /// <param name="metricFactory">A factory used to create metrics</param>
        /// <param name="fullyQualifiedNamespace">The fully qualified Event Hubs namespace to connect to. This is likely to be similar to {yournamespace}.servicebus.windows.net.</param>
        /// <param name="eventHubName">The name of the specific Event Hub to associate the processor with.</param>
        /// <param name="consumerGroupName">The name of the consumer group the processor is associated with. Events are read in the context of this group.</param>
        /// <param name="options">The set of options to use for the processor.</param>
        protected FixedProcessorClient(ILogger logger, IMetricFactory metricFactory, TokenCredential credential, string fullyQualifiedNamespace, string eventHubName, string consumerGroupName, FixedProcessorClientOptions options, ILeaseManager leaseManager, ICheckpointManager checkpointManager) : base(options?.BatchSize ?? 1, consumerGroupName, fullyQualifiedNamespace, eventHubName, credential, options)
        {
            ConfigureInstance(logger, metricFactory, options, leaseManager, checkpointManager);
        }
        #endregion
        #region Properties        
        /// <inheritdoc />
        public override int GetHashCode() => base.GetHashCode();


        /// <inheritdoc />
        public override string ToString() => base.ToString();

        /// <inheritdoc />
        public override bool Equals(object obj) => base.Equals(obj);

        /// <summary>
        /// Returns true if the instance has been initialized
        /// </summary>
        public bool IsInitialized => _initializationCount > 0;

        /// <summary>
        /// True if the processor has been started and stop has not been called
        /// </summary>
        public bool IsStarted { get; private set; }
        #endregion
        #region Methods
        private void ConfigureInstance(ILogger logger, IMetricFactory metricFactory, FixedProcessorClientOptions options, ILeaseManager leaseManager, ICheckpointManager checkpointManager)
        {
            Guard.NotNull(nameof(logger), logger);
            Guard.NotNull(nameof(leaseManager), leaseManager);
            Guard.NotNull(nameof(checkpointManager), checkpointManager);
            Guard.NotNull(nameof(options), options);
            Guard.NotNull(nameof(metricFactory), metricFactory);

            _leaseManager = leaseManager;
            _checkpointManager = checkpointManager;
            _options = options;
            _logger = logger;
            _metricFactory = metricFactory;

            _errorCounter = _metricFactory.CreateCounter("fpc-processing-error-count", "The number of processing errors raised since the start of the processor.", false, new string[0]);
            _partitionStopProcessing = _metricFactory.CreateCounter("fpc-partition-stop-count", "The number of processing stop events raised since the start of the processor.", false, new string[0]);
            _partitionInitializeProcessing = _metricFactory.CreateCounter("fpc-partition-init-count", "The number of processing initialization events raised since the start of the processor.", false, new string[0]);
        }


        /// <summary>
        /// Initializes the processor
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task InitializeAsync(CancellationToken cancellationToken)
        {            
            if (Interlocked.Increment(ref _initializationCount) == 1)
            {
                var acquiredSemaphore = false;

                if (cancellationToken.IsCancellationRequested) throw new TaskCanceledException();

                try
                {
                    await _lifecycleControl.WaitAsync(cancellationToken).ConfigureAwait(false);
                    acquiredSemaphore = true;

                    await _leaseManager.CreateStoreIfNotExistsAsync(_options, cancellationToken).ConfigureAwait(false);
                    await _checkpointManager.CreateStoreIfNotExistsAsync(_options, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException e)
                {
                    throw new TaskCanceledException("Operation Canceled while initializing.", e);
                }
                finally
                {
                    if (acquiredSemaphore)
                    {
                        _lifecycleControl.Release();
                    }
                }
            }
        }

        /// <inheritdoc />
        public override async Task StartProcessingAsync(CancellationToken cancellationToken = default)
        {
            if (!IsInitialized) throw new ApplicationException("Instance must be initialized before starting");
            if (IsStarted) throw new ApplicationException("Instance is already started");

            var acquiredSemaphore = false;

            if (cancellationToken.IsCancellationRequested) throw new TaskCanceledException();

            try
            {
                await _lifecycleControl.WaitAsync(cancellationToken).ConfigureAwait(false);
                acquiredSemaphore = true;

                await base.StartProcessingAsync(cancellationToken).ConfigureAwait(false);
                IsStarted = true;
            }
            catch(OperationCanceledException e)
            {
                throw new TaskCanceledException("Operation Canceled while starting.", e);
            }
            finally
            {
                if(acquiredSemaphore)
                {
                    _lifecycleControl.Release();
                }
            }        
        }

        /// <inheritdoc />
        public override void StartProcessing(CancellationToken cancellationToken = default)
        {
            if (!IsInitialized) throw new ApplicationException("Instance must be initialized before starting");
            if (IsStarted) throw new ApplicationException("Instance is already started");

            var acquiredSemaphore = false;

            if (cancellationToken.IsCancellationRequested) throw new TaskCanceledException();

            try
            {
                _lifecycleControl.Wait(cancellationToken);
                acquiredSemaphore = true;

                base.StartProcessing(cancellationToken);
                IsStarted = true;
            }
            catch (OperationCanceledException e)
            {
                throw new TaskCanceledException("Operation Canceled while starting.", e);
            }
            finally
            {
                if (acquiredSemaphore)
                {
                    _lifecycleControl.Release();
                }
            }
        }

        /// <inheritdoc />
        public override async Task StopProcessingAsync(CancellationToken cancellationToken = default)
        {
            if (!IsInitialized) throw new ApplicationException("Instance must be initialized before stopping");
            if (!IsStarted) throw new ApplicationException("Instance must be started before stopping");

            var acquiredSemaphore = false;

            if (cancellationToken.IsCancellationRequested) throw new TaskCanceledException();

            try
            {
                await _lifecycleControl.WaitAsync(cancellationToken).ConfigureAwait(false);
                acquiredSemaphore = true;

                await base.StopProcessingAsync(cancellationToken).ConfigureAwait(false);
                IsStarted = false;
            }
            catch (OperationCanceledException e)
            {
                throw new TaskCanceledException("Operation Canceled while stopping.", e);
            }
            finally
            {
                if (acquiredSemaphore)
                {
                    _lifecycleControl.Release();
                }
            }
        }

        /// <inheritdoc />
        public override void StopProcessing(CancellationToken cancellationToken = default)
        {
            if (!IsInitialized) throw new ApplicationException("Instance must be initialized before stopping");
            if (!IsStarted) throw new ApplicationException("Instance must be started before stopping");

            var acquiredSemaphore = false;

            if (cancellationToken.IsCancellationRequested) throw new TaskCanceledException();

            try
            {
                _lifecycleControl.Wait(cancellationToken);
                acquiredSemaphore = true;

                base.StopProcessing(cancellationToken);
                IsStarted = false;
            }
            catch (OperationCanceledException e)
            {
                throw new TaskCanceledException("Operation Canceled while stopping.", e);
            }
            finally
            {
                if (acquiredSemaphore)
                {
                    _lifecycleControl.Release();
                }
            }
        }

        /// <inheritdoc />
        protected override Task<IEnumerable<EventProcessorPartitionOwnership>> ClaimOwnershipAsync(IEnumerable<EventProcessorPartitionOwnership> desiredOwnership, CancellationToken cancellationToken)
        {
            return _leaseManager.ClaimOwnershipAsync(desiredOwnership, cancellationToken);
        }

        /// <inheritdoc />
        protected override Task<IEnumerable<EventProcessorCheckpoint>> ListCheckpointsAsync(CancellationToken cancellationToken)
        {
            return _checkpointManager.ListCheckpointsAsync(cancellationToken);
        }

        /// <inheritdoc />
        protected override Task<IEnumerable<EventProcessorPartitionOwnership>> ListOwnershipAsync(CancellationToken cancellationToken)
        {
            return _leaseManager.ListOwnershipAsync(cancellationToken);
        }




        protected sealed override async Task OnProcessingErrorAsync(Exception exception, EventProcessorPartition partition, string operationDescription, CancellationToken cancellationToken)
        {
            _errorCounter.Increment();

            using (_logger.BeginScope("Processor Error"))
            {
                _logger.LogError(exception, "Processing error received on partition {partitionId}", partition.PartitionId);

                if (_partitionProcessors.TryGetValue(partition.PartitionId, out var processor) && _partitionContexts.TryGetValue(partition.PartitionId, out var context))
                {
                    await processor.PartitionHandleErrorAsync(exception, context, operationDescription, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    _logger.LogError(exception, "Processing error received on partition {partitionId}. Could not notify instance", partition.PartitionId);
                }
            }
        }

        protected sealed override async Task OnProcessingEventBatchAsync(IEnumerable<EventData> events, EventProcessorPartition partition, CancellationToken cancellationToken)
        {
            using (_logger.BeginScope("Processor Batch Received"))
            {
                _logger.LogDebug("Processor batch received on partition {partitionId}", partition.PartitionId);

                if (_partitionProcessors.TryGetValue(partition.PartitionId, out var processor) && _partitionContexts.TryGetValue(partition.PartitionId, out var context))
                {
                    await ProcessBatchAsync(events, processor, context, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    _logger.LogError("Processing batch received on partition {partitionId}. Could not notify instance", partition.PartitionId);
                }
            }
        }

        protected sealed override async Task OnInitializingPartitionAsync(EventProcessorPartition partition, CancellationToken cancellationToken)
        {
            _partitionInitializeProcessing.Increment();

            using (_logger.BeginScope("Processor Initialization"))
            {
                _logger.LogInformation("Processing initialization received on partition {partitionId}", partition.PartitionId);

                if (!_partitionContexts.TryGetValue(partition.PartitionId, out var context))
                {
                    context = new ProcessorPartitionContext(_logger, _metricFactory, partition.PartitionId, () => ReadLastEnqueuedEventProperties(partition.PartitionId), _checkpointManager);
                    _partitionContexts.TryAdd(partition.PartitionId, context);
                }

                if (!_partitionProcessors.TryGetValue(partition.PartitionId, out var processor))
                {
                    if (_partitionContexts.TryGetValue(partition.PartitionId, out var partitionContext))
                    {
                        processor = CreateProcessor(_logger, _metricFactory, partitionContext);

                        await processor.InitializeAsync(_logger, _metricFactory, partitionContext).ConfigureAwait(false);
                        _partitionProcessors.TryAdd(partition.PartitionId, processor);
                    }
                    else
                    {
                        _logger.LogError("Error retrieving context from context list for partition {partitionId}", partition.PartitionId);
                        throw new ApplicationException($"Error retriving partition from list for partition {partition.PartitionId}");
                    }
                }

                await processor.PartitionInitializeAsync(context, cancellationToken).ConfigureAwait(false);
            }
        }

        protected sealed override async Task OnPartitionProcessingStoppedAsync(EventProcessorPartition partition, ProcessingStoppedReason reason, CancellationToken cancellationToken)
        {
            using (_logger.BeginScope("Processor Stop"))
            {
                _logger.LogInformation("Processing stopped received on partition {partitionId}", partition.PartitionId);

                if (_partitionProcessors.TryGetValue(partition.PartitionId, out var processor) && _partitionContexts.TryGetValue(partition.PartitionId, out var context))
                {
                    await processor.PartitionStopAsync(context, reason, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    _logger.LogError("Processing stopped received on partition {partitionId}. Could not notify instance", partition.PartitionId);
                }
            }
        }

        protected abstract IProcessor CreateProcessor(ILogger logger, IMetricFactory metricFactory, ProcessorPartitionContext partitionContext);

        protected abstract Task ProcessBatchAsync(IEnumerable<EventData> events, IProcessor processor, ProcessorPartitionContext partitionContext, CancellationToken cancellationToken);
        #endregion
        #region Safe Disposal
        /// <summary>
        /// Cleans up resources as required
        /// </summary>
        ~FixedProcessorClient()
        {
            Dispose(false);
        }
        

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Common dispose method used to cleanup resources
        /// </summary>
        /// <param name="isDisposing">True if invoked from the Dispose() method</param>
        private void Dispose(bool isDisposing)
        {
            if(Interlocked.Increment(ref _disposalCount) == 1)
            {
                if (isDisposing) GC.SuppressFinalize(this);

                _lifecycleControl.Dispose();
            }
        }
        #endregion
    }
}
