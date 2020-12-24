// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer
{
    #region Using Clauses
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    #endregion

    /// <summary>
    /// Executes a task if it is runnable based on the execution policy conditions
    /// </summary>
    public interface IConcurrencyPolicy
    {
        #region Delegates
        /// <summary>
        /// The method that will be invoked to retrieve the processing task
        /// </summary>
        /// <param name="data">The event data to be processed</param>
        /// <param name="state">User state</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>The event data that was processed</returns>
        delegate Task<EventData> ProcessData(EventData data, object state, CancellationToken cancellationToken);
        #endregion
        #region Properties
        /// <summary>
        /// The number of tasks currently exeucting
        /// </summary>
        int Count { get; }

        /// <summary>
        /// The maximum number of tasks that can operate at any given time
        /// </summary>
        int Capacity { get; }
        #endregion
        #region Methods        
        /// <summary>
        /// Schedules a task for monitoring
        /// </summary>
        /// <param name="data">The event data associated with the tracked task</param>
        /// <param name="processor">The delegate that will be invoked if processing fits within the concurrency rules</param>
        /// <param name="state">User state</param>
        /// <param name="cancellationToken">A token to monitor for abort and cancellationr requests</param>
        /// <returns>The task if it is allowed to run based on the concurrency policy, null if not</returns>
        Task<Task<EventData>> RunAsync(EventData data, ProcessData processor, object state, CancellationToken cancellationToken);
        #endregion
    }
}
