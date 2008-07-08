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
        private object _logFileLock = new object();
        private static log4net.ILog _log = log4net.LogManager.GetLogger(typeof(EditLog));
        private bool _loggingEnabled = true;

        /// <summary>
        /// Initializes a new instance of the <see cref="EditLog"/> class.
        /// </summary>
        /// <param name="appendLog"><see cref="true"/> to continue an existing log file; <see cref="false"/> to create a new one.</param>
        public EditLog(bool appendLog)
        {
            if( !appendLog )
                System.IO.File.Delete("EditLog.log");
        }

        /// <summary>
        /// Log a file system mutation to the edit log.
        /// </summary>
        /// <param name="mutation">The mutation that took place.</param>
        /// <param name="path">The DFS path that was changed.</param>
        public void LogMutation(FileSystemMutation mutation, string path, DateTime date)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            if( _loggingEnabled )
            {
                try
                {
                    lock( _logFileLock )
                    {
                        using( TextWriter writer = new StreamWriter("EditLog.log", true) )
                        {
                            writer.WriteLine(string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}:{1:yyyyMMddHHmmss.fffffff}:{2}", mutation, date, path));
                        }
                    }
                }
                catch( IOException ex )
                {
                    HandleLoggingError(ex);
                    throw;
                }
            }
        }

        /// <summary>
        /// Replays the log file.
        /// </summary>
        public void ReplayLog(FileSystem fileSystem)
        {
            try
            {
                _loggingEnabled = false;
                if( File.Exists("EditLog.log") )
                {
                    using( TextReader reader = System.IO.File.OpenText("EditLog.log") )
                    {
                        // TODO: Get the actual root creation time from somewhere.
                        string line;
                        while( (line = reader.ReadLine()) != null )
                        {
                            string[] parts = line.Split(':');
                            FileSystemMutation mutation = (FileSystemMutation)Enum.Parse(typeof(FileSystemMutation), parts[0]);
                            DateTime date = DateTime.ParseExact(parts[1], "yyyyMMddHHmmss.fffffff", System.Globalization.CultureInfo.InvariantCulture);
                            switch( mutation )
                            {
                            case FileSystemMutation.CreateDirectory:
                                fileSystem.CreateDirectory(parts[2], date);
                                break;
                            case FileSystemMutation.CreateFile:
                                fileSystem.CreateFile(parts[2], date);
                                break;
                            }
                        }
                    }
                }
            }
            finally
            {
                _loggingEnabled = true;
            }
        }

        private void HandleLoggingError(Exception ex)
        {
            _log.Error("Unable to log file system mutation.", ex);
        }
    }
}
