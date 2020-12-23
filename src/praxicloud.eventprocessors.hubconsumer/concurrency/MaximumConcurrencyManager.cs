// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.concurrency
{
    #region Using Clauses
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using praxicloud.core.security;
    #endregion

    /// <summary>
    /// Manages concurrency to allow a maximum number of concurrently executing tasks
    /// </summary>
    public sealed class MaximumConcurrencyManager : IConcurrencyManager, IDisposable
    {
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
        /// A queue of completed tasks for retrieval
        /// </summary>
        private readonly ConcurrentQueue<Task<EventData>> _completedTasks = new ConcurrentQueue<Task<EventData>>();
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="capacity">The maximum concurrency allowed</param>
        /// <param name="trackCompleted">True if the completed events should be tracked until returned</param>
        public MaximumConcurrencyManager(int capacity, bool trackCompleted)
        {
            Guard.NotLessThan(nameof(capacity), capacity, 1);

            Capacity = capacity;
            _concurrencyCount = new SemaphoreSlim(capacity);
            TrackCompleted = trackCompleted;
            _trackedTasks = new ConcurrentDictionary<long, Task<Task<EventData>>>(5, capacity);
        }

        /// <summary>
        /// Finalizer
        /// </summary>
        ~MaximumConcurrencyManager()
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

            if(await _concurrencyCount.WaitAsync(timeout, cancellationToken).ConfigureAwait(false))
            {
                _trackedTasks.TryAdd(task.Id, task.ContinueWith(t =>
                {                   
                    try
                    {
                        if(_trackedTasks.TryRemove(task.Id, out var trackedElement))
                        {
                            if(TrackCompleted) _completedTasks.Enqueue(t);
                        }
                    }
                    finally
                    {
                        _concurrencyCount.Release();
                    }

                    return t;
                }));

                scheduled = true;
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