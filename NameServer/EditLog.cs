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

        public void LogCreateDirectory(string path, DateTime date)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            LogMutation("{0}:{1:yyyyMMddHHmmss.fffffff}:{2}", FileSystemMutation.CreateDirectory, date, path);
        }

        public void LogCreateFile(string path, DateTime date)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            LogMutation("{0}:{1:yyyyMMddHHmmss.fffffff}:{2}", FileSystemMutation.CreateFile, date, path);
        }

        public void LogAppendBlock(string path, DateTime date, Guid blockId)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            LogMutation("{0}:{1:yyyyMMddHHmmss.fffffff}:{2}:{3}", FileSystemMutation.AppendBlock, date, path, blockId);
        }

        public void LogCommitBlock(string path, DateTime date, Guid blockId, int size)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            LogMutation("{0}:{1:yyyyMMddHHmmss.fffffff}:{2}:{3}:{4}", FileSystemMutation.CommitBlock, date, path, blockId, size);
        }

        public void LogCommitFile(string path)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            LogMutation("{0}:{1:yyyyMMddHHmmss.fffffff}:{2}", FileSystemMutation.CommitFile, DateTime.UtcNow, path);
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
                                fileSystem.CreateFile(parts[2], date, false);
                                break;
                            case FileSystemMutation.AppendBlock:
                                fileSystem.AppendBlock(parts[2], new Guid(parts[3]), false);
                                break;
                            case FileSystemMutation.CommitBlock:
                                fileSystem.CommitBlock(parts[2], new Guid(parts[3]), Convert.ToInt32(parts[4]));
                                break;
                            case FileSystemMutation.CommitFile:
                                fileSystem.CloseFile(parts[2]);
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

        private void LogMutation(string format, params object[] parameters)
        {
            if( _loggingEnabled )
            {
                try
                {
                    lock( _logFileLock )
                    {
                        using( TextWriter writer = new StreamWriter("EditLog.log", true) )
                        {
                            writer.WriteLine(string.Format(System.Globalization.CultureInfo.InvariantCulture, format, parameters));
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
    }
}
