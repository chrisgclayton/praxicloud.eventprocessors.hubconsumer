// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer.concurrency
{
    #region Using Clauses
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    #endregion

    /// <summary>
    /// Manages concurrency to allow a maximum number of concurrently executing tasks
    /// </summary>
    public sealed class SingleConcurrencyPolicy : IConcurrencyPolicy, IDisposable
    {
        #region Variables
        /// <summary>
        /// A control used to synchronize access to run method
        /// </summary>
        private readonly SemaphoreSlim _runControl = new SemaphoreSlim(1);

        /// <summary>
        /// The tasks currently being tracked
        /// </summary>
        private Task<EventData> _trackedTask;

        /// <summary>
        /// The number of times the instance has been disposed of
        /// </summary>
        private int _disposalCount;
        #endregion
        #region Constructors
        /// <summary>
        /// Finalizer
        /// </summary>
        ~SingleConcurrencyPolicy()
        {
            Dispose(false);
        }
        #endregion
        #region Properties
        /// <inheritdoc />
        public int Count => _trackedTask == null ? 0 : 1;

        /// <inheritdoc />
        public int Capacity => 1;
        #endregion
        #region Methods
        /// <inheritdoc />
        public async Task<Task<EventData>> RunAsync(EventData data, IConcurrencyPolicy.ProcessData processor, object state, CancellationToken cancellationToken)
        {
            Task<EventData> executionTask = null;

            await _runControl.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                if(_trackedTask == null)
                {
                    executionTask = processor(data, state, cancellationToken);
                    _trackedTask = executionTask;
                    _ = _trackedTask.ContinueWith(t => _trackedTask = null);
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
            if(Interlocked.Increment(ref _disposalCount) == 1)
            {
                if (isDisposing) GC.SuppressFinalize(this);

                _runControl.Dispose();

            }
        }
        #endregion
    }
}

