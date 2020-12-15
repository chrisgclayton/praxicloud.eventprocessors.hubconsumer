// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.hubconsumer
{
    #region Using Clauses
    using System;
    using System.Collections.Generic;
    #endregion

    /// <summary>
    /// A processing exception that raises more details than the aggregate exception, allowing for sequence number alignment
    /// </summary>
    public class HubProcessingException : AggregateException
    {
        #region Constructors
        /// <summary>
        /// Initializes a new instance of the type
        /// </summary>
        /// <param name="message">The message associated with the exception</param>
        /// <param name="exceptions">The exceptions that occurred, keyed by the squence number</param>
        public HubProcessingException(string message, Dictionary<long, Exception> exceptions) : base(message, exceptions.Values)
        {
            SequenceExceptions = exceptions;
        }
        #endregion
        #region Properties
        /// <summary>
        /// A list of exceptions keyed by sequence number with -1 being null message from receive timeout
        /// </summary>
        public Dictionary<long, Exception> SequenceExceptions { get; }
        #endregion
    }
}
