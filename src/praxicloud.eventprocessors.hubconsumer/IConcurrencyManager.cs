// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer
{
    #region Using Clauses
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using System.Collections.Generic;
    using System;
    #endregion

    /// <summary>
    /// An instance that manages the running tasks
    /// </summary>
    public interface IConcurrencyManager
    {
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
        #region Properties
        /// <summary>
        /// True if the completed events should be tracked until returned. If this is set to tru the wait / read tasks will not return any results but will wait for the executing tasks to perform appropriate operations. If true
        /// the consumer commits to remove tasks and they will be held until received
        /// </summary>
        bool TrackCompleted { get; }
        #endregion
        #region Methods        
        /// <summary>
        /// Schedules a task for monitoring
        /// </summary>
        /// <param name="data">The event data associated with the tracked task</param>
        /// <param name="task">The task to monitor</param>
        /// <param name="timeout">The maximum time to attempt to add the task</param>
        /// <param name="cancellationToken">A token to monitor for abort and cancellationr equests</param>
        /// <returns>True if the task was able to be scheduled, false if it exceeds the maximum executing count</returns>
        Task<bool> ScheduleAsync(EventData data, Task<EventData> task, TimeSpan timeout, CancellationToken cancellationToken);

        /// <summary>
        /// Waits for all currently executing tasks to complete and returns a list of all completed tasks.
        /// </summary>
        /// <param name="timeout">The maximum time to wait. This is best effort depending on the implementation</param>
        /// <param name="cancellation">A token to monitor for abort requests</param>
        /// <returns>A list of the completed tasks. If multiple wait operations are run concurrently there tasks returned may not be the same as the ones that completed from the time the wait was initiated</returns>
        Task<List<Task<EventData>>> WhenAllAsync(TimeSpan timeout, CancellationToken cancellation);

        /// <summary>
        /// Waits for an event to complete and returns one or more completed tasks.
        /// </summary>
        /// <param name="timeout">The maximum time to wait. This is best effort depending on the implementation</param>
        /// <param name="cancellationToken">A token to monitor for abort requests</param>
        /// <returns>A task that completed or null is possible if concurrent read operations are being performed</returns>
        Task<Task<EventData>> WhenAnyAsync(TimeSpan timeout, CancellationToken cancellationToken);
        #endregion
    }
}
