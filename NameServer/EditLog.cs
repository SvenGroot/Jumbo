﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace NameServerApplication
{
    /// <summary>
    /// Represents an edit log file for the file system.
    /// </summary>
    class EditLog
    {
        private object _logFileLock = new object();
        private static log4net.ILog _log = log4net.LogManager.GetLogger(typeof(EditLog));
        private bool _loggingEnabled = true;
        private string _logFilePath;

        /// <summary>
        /// Initializes a new instance of the <see cref="EditLog"/> class.
        /// </summary>
        /// <param name="appendLog"><see cref="true"/> to continue an existing log file; <see cref="false"/> to create a new one.</param>
        public EditLog(bool appendLog, string logFileDirectory)
        {
            if( logFileDirectory == null )
                logFileDirectory = string.Empty;
            if( logFileDirectory.Length > 0 )
                System.IO.Directory.CreateDirectory(logFileDirectory);
            _logFilePath = Path.Combine(logFileDirectory, "EditLog.log");
            if( !appendLog )
                System.IO.File.Delete(_logFilePath);
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

        public void LogCommitFile(string path, bool discardPendingBlocks)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            LogMutation("{0}:{1:yyyyMMddHHmmss.fffffff}:{2}:{3}", FileSystemMutation.CommitFile, DateTime.UtcNow, path, discardPendingBlocks);
        }

        public void LogDelete(string path, bool recursive)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            LogMutation("{0}:{1:yyyyMMddHHmmss.fffffff}:{2}:{3}", FileSystemMutation.Delete, DateTime.UtcNow, path, recursive);
        }

        public void LogMove(string from, string to)
        {
            if( from == null )
                throw new ArgumentNullException("from");
            if( to == null )
                throw new ArgumentNullException("to");

            LogMutation("{0}:{1:yyyyMMddHHmmss.fffffff}:{2}:{3}", FileSystemMutation.Move, DateTime.UtcNow, from, to);
        }

        /// <summary>
        /// Replays the log file.
        /// </summary>
        public void ReplayLog(FileSystem fileSystem)
        {
            try
            {
                _loggingEnabled = false;
                if( File.Exists(_logFilePath) )
                {
                    using( TextReader reader = System.IO.File.OpenText(_logFilePath) )
                    {
                        // TODO: Get the actual root creation time from somewhere.
                        string line;
                        while( (line = reader.ReadLine()) != null )
                        {
                            string[] parts = line.Split(':');
                            FileSystemMutation mutation = (FileSystemMutation)Enum.Parse(typeof(FileSystemMutation), parts[0]);
                            DateTime date = DateTime.ParseExact(parts[1], "yyyyMMddHHmmss.fffffff", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal);
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
                                fileSystem.CloseFile(parts[2], Convert.ToBoolean(parts[3]));
                                break;
                            case FileSystemMutation.Delete:
                                fileSystem.Delete(parts[2], Convert.ToBoolean(parts[3]));
                                break;
                            case FileSystemMutation.Move:
                                fileSystem.Move(parts[2], parts[3]);
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
                        using( TextWriter writer = new StreamWriter(_logFilePath, true) )
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
