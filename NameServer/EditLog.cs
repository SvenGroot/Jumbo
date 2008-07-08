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
    class EditLog : IDisposable
    {
        private TextWriter _logFile;
        private object _logFileLock = new object();
        private static log4net.ILog _log = log4net.LogManager.GetLogger(typeof(EditLog));

        /// <summary>
        /// Initializes a new instance of the <see cref="EditLog"/> class.
        /// </summary>
        /// <param name="fileSystem">The file system that this class is logging for.</param>
        /// <param name="replayLog"><see langword="true" /> if you wish to recreate the file system from the log file; <see langword="false" /> if you wish to start a new file system (this erases the old log file, if any).</param>
        public EditLog(FileSystem fileSystem, bool replayLog)
        {
            if( fileSystem == null )
                throw new ArgumentNullException("fileSystem");

            if( replayLog )
                ReplayLog(fileSystem);
            else
                System.IO.File.Delete("EditLog.log");

            _logFile = new StreamWriter("EditLog.log", true);
        }

        /// <summary>
        /// Log a file system mutation to the edit log.
        /// </summary>
        /// <param name="mutation">The mutation that took place.</param>
        /// <param name="path">The DFS path that was changed.</param>
        public void LogMutation(FileSystemMutation mutation, string path)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            if( _logFile != null )
            {
                try
                {
                    lock( _logFileLock )
                    {
                        _logFile.WriteLine("{0}:{1}", mutation, path);
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
        /// Replays the log file and returns the resulting file system.
        /// </summary>
        private static void ReplayLog(FileSystem fileSystem)
        {
            using( TextReader reader = System.IO.File.OpenText("EditLog.log") )
            {
                // TODO: Get the actual root creation time from somewhere.
                string line;
                while( (line = reader.ReadLine()) != null )
                {
                    string[] parts = line.Split(':');
                    FileSystemMutation mutation = (FileSystemMutation)Enum.Parse(typeof(FileSystemMutation), parts[0]);
                    switch( mutation )
                    {
                    case FileSystemMutation.CreateDirectory:
                        fileSystem.CreateDirectory(parts[1]);
                        break;
                    case FileSystemMutation.CreateFile:
                        fileSystem.CreateFile(parts[1]);
                        break;
                    }
                }
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if( disposing )
                _logFile.Dispose();
        }

        private void HandleLoggingError(Exception ex)
        {
            _log.Error("Unable to log file system mutation.", ex);
        }

        #region IDisposable Members

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion
    }
}
