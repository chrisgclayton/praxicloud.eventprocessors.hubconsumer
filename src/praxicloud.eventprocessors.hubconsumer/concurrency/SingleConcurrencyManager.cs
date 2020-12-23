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
    #endregion

    /// <summary>
    /// A concurrency manager that only allows for one task to execute at a time
    /// </summary>
    public sealed class SingleConcurrencyManager : IConcurrencyManager, IDisposable
    {
        #region Variables
        /// <summary>
        /// The number of times the instance has been disposed of
        /// </summary>
        private long _disposalCount;

        /// <summary>
        /// A control that serializes scheduling requests
        /// </summary>
        private SemaphoreSlim _scheduleControl = new SemaphoreSlim(1);

        /// <summary>
        /// A control that serializes read access
        /// </summary>
        private SemaphoreSlim _readControl = new SemaphoreSlim(1);

        /// <summary>
        /// The currently tracked task
        /// </summary>
        private Task<Task<EventData>> _trackedTask = null;

        /// <summary>
        /// A queue of completed tasks
        /// </summary>
        private ConcurrentQueue<Task<EventData>> _completedTasks = new ConcurrentQueue<Task<EventData>>();
        #endregion
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="trackCompleted">True if the completed events should be tracked until returned. If this is set to tru the wait / read tasks will not return any results but will wait for the executing tasks to perform appropriate operations. If true the consumer commits to remove tasks and they will be held until received</param>
        public SingleConcurrencyManager(bool trackCompleted)
        {
            TrackCompleted = trackCompleted;
        }

        /// <summary>
        /// Finalizer
        /// </summary>
        ~SingleConcurrencyManager()
        {
            Dispose(false);
        }
        #endregion
        #region Properties
        /// <inheritdoc />
        public int Count => _trackedTask?.IsCompleted ?? true ? 0 : 1;

        /// <inheritdoc />
        public int Capacity => 1;

        /// <inheritdoc />
        public bool TrackCompleted { get; }
        #endregion
        #region Methods
        /// <inheritdoc />
        public async Task<bool> ScheduleAsync(EventData data, Task<EventData> task, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var scheduled = false;

            if (Count < 1)
            {
                if(await _scheduleControl.WaitAsync(timeout, cancellationToken).ConfigureAwait(false))
                {
                    if(Count < 1)
                    {
                        _trackedTask = task.ContinueWith(t =>
                        {
                            _completedTasks.Enqueue(t);
                            _trackedTask = null;

                            return t;
                        });

                        scheduled = true;
                    }

                    _scheduleControl.Release();
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
                        if (_trackedTask != null)
                        {
                            _ = await Task.WhenAll(_trackedTask).ConfigureAwait(false);
                        }

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
                _ = await Task.WhenAll(_trackedTask).ConfigureAwait(false);
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
                            if (_trackedTask != null)
                            {
                                _ = await Task.WhenAny(_trackedTask).ConfigureAwait(false);
                            }

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
                _ = await Task.WhenAny(_trackedTask).ConfigureAwait(false);
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
                _scheduleControl.Dispose();
            }
        }
        #endregion
    }
}
