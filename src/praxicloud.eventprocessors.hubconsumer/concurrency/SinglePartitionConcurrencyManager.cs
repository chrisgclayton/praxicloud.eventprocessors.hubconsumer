// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.concurrency
{
    #region Using Clauses
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Runtime.ConstrainedExecution;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using praxicloud.core.security;
    #endregion

    /// <summary>
    /// Manages concurrency to allow a maximum number of concurrently executing tasks
    /// </summary>
    public sealed class SinglePartitionConcurrencyManager : IConcurrencyManager, IDisposable
    {
        #region Delegates
        /// <summary>
        /// Determeines the partition a message belongs to
        /// </summary>
        /// <param name="data">The message</param>
        /// <returns>The partition the message belongs to</returns>
        public delegate string PartitionCalculator(EventData data);
        #endregion
        #region Variables
        /// <summary>
        /// The number of times the instance has been disposed of
        /// </summary>
        private long _disposalCount;

        /// <summary>
        /// Manages the number of tracked tasks
        /// </summary>
        private readonly SemaphoreSlim _concurrencyCount;

        /// <summary>
        /// Synchronizes access to the read operations
        /// </summary>
        private readonly SemaphoreSlim _readControl = new SemaphoreSlim(1);

        /// <summary>
        /// The list of tasks that are currently executing
        /// </summary>
        private readonly ConcurrentDictionary<long, Task<Task<EventData>>> _trackedTasks;

        /// <summary>
        /// A list of partitions currently executing
        /// </summary>
        private readonly ConcurrentDictionary<string, string> _trackedPartitions;

        /// <summary>
        /// A queue of completed tasks for retrieval
        /// </summary>
        private readonly ConcurrentQueue<Task<EventData>> _completedTasks = new ConcurrentQueue<Task<EventData>>();

        /// <summary>
        /// An instance that determines the partition an event belongs to
        /// </summary>
        private readonly IExecutionPartitioner _partitionerInstance;

        /// <summary>
        /// The partitioner method
        /// </summary>
        private readonly PartitionCalculator _partitioner;

        /// <summary>
        /// True if the partition is case sensitive
        /// </summary>
        private readonly bool _isPartitionCaseSensitive;
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="capacity">The maximum concurrency allowed</param>
        /// <param name="trackCompleted">True if the completed events should be tracked until returned</param>
        /// <param name="partitioner">An instance that determines the partition an event belongs to</param>
        public SinglePartitionConcurrencyManager(int capacity, bool trackCompleted, IExecutionPartitioner partitioner) : this(capacity, trackCompleted, partitioner.GetPartition, partitioner.IsCaseSensitive)
        {
        }

        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="capacity">The maximum concurrency allowed</param>
        /// <param name="trackCompleted">True if the completed events should be tracked until returned</param>
        public SinglePartitionConcurrencyManager(int capacity, bool trackCompleted, PartitionCalculator partitioner, bool isPartitionCaseSensitive)
        {
            Guard.NotLessThan(nameof(capacity), capacity, 1);
            Guard.NotNull(nameof(partitioner), partitioner);

            Capacity = capacity;
            _concurrencyCount = new SemaphoreSlim(capacity);
            TrackCompleted = trackCompleted;
            _trackedTasks = new ConcurrentDictionary<long, Task<Task<EventData>>>(5, capacity);
            _trackedPartitions = new ConcurrentDictionary<string, string>(5, capacity);

            _partitioner = partitioner;
            _isPartitionCaseSensitive = isPartitionCaseSensitive;
        }

        /// <summary>
        /// Finalizer
        /// </summary>
        ~SinglePartitionConcurrencyManager()
        {
            Dispose(false);
        }
        #endregion
        #region Properties
        /// <inheritdoc />
        public int Count => Capacity - _concurrencyCount.CurrentCount;

        /// <inheritdoc />
        public int Capacity { get; }

        /// <inheritdoc />
        public bool TrackCompleted { get; }
        #endregion
        #region Methods
        /// <inheritdoc />
        public async Task<bool> ScheduleAsync(EventData data, Task<EventData> task, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var scheduled = false;

            if (await _concurrencyCount.WaitAsync(timeout, cancellationToken).ConfigureAwait(false))
            {
                var partition = _partitioner(data);

                if (!_trackedPartitions.ContainsKey(partition))
                {
                    _trackedPartitions.TryAdd(partition, partition);
                    _trackedTasks.TryAdd(task.Id, task.ContinueWith(t =>
                     {
                         try
                         {
                             if (_trackedTasks.TryRemove(task.Id, out var trackedElement))
                             {
                                 if (TrackCompleted) _completedTasks.Enqueue(t);
                             }

                             _trackedPartitions.TryRemove(partition, out _);
                         }
                         finally
                         {
                             _concurrencyCount.Release();
                         }

                         return t;
                     }));

                    scheduled = true;
                }
            }

            return scheduled;
        }

        /// <inheritdoc />
        public async Task<List<Task<EventData>>> WhenAllAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            List<Task<EventData>> results = null;

            if (TrackCompleted)
            {
                if (await _readControl.WaitAsync(timeout, cancellationToken).ConfigureAwait(false))
                {
                    try
                    {
                        _ = await Task.WhenAll(_trackedTasks.Values).ConfigureAwait(false);

                        results = new List<Task<EventData>>(_completedTasks.Count);

                        while (_completedTasks.TryDequeue(out var completed))
                        {
                            results.Add(completed);
                        }

                    }
                    finally
                    {
                        _readControl.Release();
                    }

                }
            }
            else
            {
                _ = await Task.WhenAll(_trackedTasks.Values).ConfigureAwait(false);
            }

            return results;
        }

        /// <inheritdoc />
        public async Task<Task<EventData>> WhenAnyAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            Task<EventData> results = null;

            if (TrackCompleted)
            {
                if (await _readControl.WaitAsync(timeout, cancellationToken).ConfigureAwait(false))
                {
                    try
                    {
                        if (!_completedTasks.TryDequeue(out var completed))
                        {
                            _ = await Task.WhenAny(_trackedTasks.Values).ConfigureAwait(false);

                            if (_completedTasks.TryDequeue(out completed))
                            {
                                results = completed;
                            }
                        }
                        else
                        {
                            results = completed;
                        }
                    }
                    finally
                    {
                        _readControl.Release();
                    }
                }
            }
            else
            {
                _ = await Task.WhenAny(_trackedTasks.Values).ConfigureAwait(false);
            }

            return results;
        }
        #endregion
        #region Safe Disposal Pattern
        public void Dispose()
        {
            Dispose(true);
        }

        private void Dispose(bool isDisposing)
        {
            if (Interlocked.Increment(ref _disposalCount) == 1)
            {
                if (isDisposing)
                {
                    GC.SuppressFinalize(this);
                }

                _readControl.Dispose();
                _concurrencyCount.Dispose();
            }
        }
        #endregion
    }
}