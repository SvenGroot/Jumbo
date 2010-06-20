﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides information about the read and write operations done by a task.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   The <see cref="InputRecords"/>, <see cref="InputBytes"/>, <see cref="OutputRecords"/> and <see cref="OutputBytes"/>
    ///   properties provide information about the amount of data processed and generated by this task. They do not take
    ///   compression or the source or destination of the data into account.
    /// </para>
    /// <para>
    ///   The remaining properties provide information about the amount of I/O activity performed by the task. For
    ///   instance <see cref="LocalBytesRead"/> tells you how much data was read from the local disk. This can include
    ///   data that was first written to the disk by a channel or record reader and then read again. Because of this
    ///   and things like compression, this number doesn't need to match <see cref="InputBytes"/>.
    /// </para>
    /// </remarks>
    [Serializable]
    public class TaskMetrics
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskMetrics));

        /// <summary>
        /// Gets or sets the number of bytes read from the Distributed File System.
        /// </summary>
        public long DfsBytesRead { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes written to the Distributed File System.
        /// </summary>
        public long DfsBytesWritten { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes read from the local disk.
        /// </summary>
        public long LocalBytesRead { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes written to the local disk.
        /// </summary>
        public long LocalBytesWritten { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes read over the network by the file and TCP channels.
        /// </summary>
        public long NetworkBytesRead { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes written over the network by the TCP channel.
        /// </summary>
        /// <value>The network bytes written.</value>
        public long NetworkBytesWritten { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes that this task had as input.
        /// </summary>
        /// <value>The input bytes.</value>
        public long InputBytes { get; set; }

        /// <summary>
        /// Gets or sets the number of records read.
        /// </summary>
        public long InputRecords { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes that this task had as output.
        /// </summary>
        /// <value>The output bytes.</value>
        public long OutputBytes { get; set; }

        /// <summary>
        /// Gets or sets the number of records written.
        /// </summary>
        public long OutputRecords { get; set; }

        /// <summary>
        /// Returns a string representation of the <see cref="TaskMetrics"/> object.
        /// </summary>
        /// <returns>A string representation of the <see cref="TaskMetrics"/> object.</returns>
        public override string ToString()
        {
            StringWriter result = new StringWriter();
            result.WriteLine("Input records: {0}", InputRecords);
            result.WriteLine("Output records: {0}", OutputRecords);
            result.WriteLine("Input bytes: {0}", InputBytes);
            result.WriteLine("Output bytes: {0}", OutputBytes);
            result.WriteLine("DFS bytes read: {0}", DfsBytesRead);
            result.WriteLine("DFS bytes written: {0}", DfsBytesWritten);
            result.WriteLine("Local bytes read: {0}", LocalBytesRead);
            result.WriteLine("Local bytes written: {0}", LocalBytesWritten);
            result.WriteLine("Channel network bytes read: {0}", NetworkBytesRead);
            result.WriteLine("Channel network bytes written: {0}", NetworkBytesWritten);
            return result.ToString();
        }

        /// <summary>
        /// Writes the metrics to the log.
        /// </summary>
        public void LogMetrics()
        {
            _log.InfoFormat("Input records: {0}", InputRecords);
            _log.InfoFormat("Output records: {0}", OutputRecords);
            _log.InfoFormat("Input bytes: {0}", InputBytes);
            _log.InfoFormat("Output bytes: {0}", OutputBytes);
            _log.InfoFormat("DFS bytes read: {0}", DfsBytesRead);
            _log.InfoFormat("DFS bytes written: {0}", DfsBytesWritten);
            _log.InfoFormat("Local bytes read: {0}", LocalBytesRead);
            _log.InfoFormat("Local bytes written: {0}", LocalBytesWritten);
            _log.InfoFormat("Channel network bytes read: {0}", NetworkBytesRead);
            _log.InfoFormat("Channel network bytes written: {0}", NetworkBytesWritten);
        }
    }
}
