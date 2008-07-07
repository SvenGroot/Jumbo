using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace NameServer
{
    /// <summary>
    /// Represents an edit log file for the file system.
    /// </summary>
    class EditLog
    {
        private TextWriter _logFile = System.IO.File.CreateText("EditLog.log");
        private object _logFileLock = new object();

    }
}
