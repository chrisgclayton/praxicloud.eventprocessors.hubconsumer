﻿// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.concurrency
{
    #region Using Clauses
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using praxicloud.core.security;
    #endregion

    /// <summary>
    /// Manages concurrency to allow a maximum number of concurrently executing tasks, not allowing 2 of the same partition execute at the same time.
    /// </summary>
    public class PartitionedMaximumConcurrencyPolicy : IConcurrencyPolicy, IDisposable
    {
        #region Variables
        /// <summary>
        /// A control used to synchronize access to run method
        /// </summary>
        private readonly SemaphoreSlim _runControl = new SemaphoreSlim(1);

        /// <summary>
        /// Tracks the number of tasks in progress
        /// </summary>
        private long _executingTaskCount = 0;

        /// <summary>
        /// The partitioner to determine the partition to assign the message to
        /// </summary>
        private readonly IExecutionPartitioner _partitioner;

        /// <summary>
        /// The partitions currently executing on
        /// </summary>
        private readonly ConcurrentDictionary<string, object> _partitionTracker = new ConcurrentDictionary<string, object>();

        /// <summary>
        /// The number of times the instance has been disposed of
        /// </summary>
        private int _disposalCount;
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="maximumDegreeOfParallelism">The maximum degree of parallelism allowd</param>
        /// <param name="partitioner">A partitioning utility for messages</param>
        public PartitionedMaximumConcurrencyPolicy(short maximumDegreeOfParallelism, IExecutionPartitioner partitioner)
        {
            Guard.NotLessThan(nameof(maximumDegreeOfParallelism), maximumDegreeOfParallelism, 1);
            Guard.NotNull(nameof(partitioner), partitioner);

            Capacity = maximumDegreeOfParallelism;
            _partitioner = partitioner;
        }

        /// <summary>
        /// Finalizer
        /// </summary>
        ~PartitionedMaximumConcurrencyPolicy()
        {
            Dispose(false);
        }
        #endregion
        #region Properties
        /// <inheritdoc />
        public int Count => (int)Interlocked.Read(ref _executingTaskCount);

        /// <inheritdoc />
        public int Capacity { get; }
        #endregion
        #region Methods
        /// <inheritdoc />
        public async Task<Task<EventData>> RunAsync(EventData data, IConcurrencyPolicy.ProcessData processor, object state, CancellationToken cancellationToken)
        {
            Task<EventData> executionTask = null;

            await _runControl.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                if (Interlocked.Read(ref _executingTaskCount) <= Capacity)
                {
                    var partition = _partitioner.GetPartition(data);
                    if (!_partitioner.IsCaseSensitive) partition = partition.ToLowerInvariant();

                    if(_partitionTracker.TryAdd(partition, partition))
                    {
                        Interlocked.Increment(ref _executingTaskCount);
                        executionTask = processor(data, state, cancellationToken);
                        _ = executionTask.ContinueWith(t =>
                        {
                            _partitionTracker.TryRemove(partition, out _);
                            Interlocked.Decrement(ref _executingTaskCount);
                        });
                    }
                }
            }
            finally
            {
                _runControl.Release();
            }

            return executionTask;
        }
        #endregion
        #region Safe Disposal Pattern
        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
        }

        private void Dispose(bool isDisposing)
        {
            if (Interlocked.Increment(ref _disposalCount) == 1)
            {
                if (isDisposing) GC.SuppressFinalize(this);

                _runControl.Dispose();

            }
        }
        #endregion
    }
}
