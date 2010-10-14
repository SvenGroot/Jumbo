// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo
{
    /// <summary>
    /// The type of log file of a process.
    /// </summary>
    public enum LogFileKind
    {
        /// <summary>
        /// The log file created by log4net.
        /// </summary>
        Log,
        /// <summary>
        /// The standard output.
        /// </summary>
        StdOut,
        /// <summary>
        /// The standard error.
        /// </summary>
        StdErr
    }
}
