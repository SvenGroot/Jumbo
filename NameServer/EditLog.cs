using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Dfs;

namespace NameServerApplication
{
    /// <summary>
    /// Represents an edit log file for the file system.
    /// </summary>
    sealed class EditLog : IDisposable
    {
        #region Nested types

        private abstract class EditLogEntry : IWritable
        {
            protected EditLogEntry(FileSystemMutation mutation)
            {
                Mutation = mutation;
            }

            protected EditLogEntry(FileSystemMutation mutation, DateTime date, string path)
            {
                Mutation = mutation;
                Date = date;
                Path = path;
            }

            public FileSystemMutation Mutation { get; private set; }

            public DateTime Date { get; private set; }

            public string Path { get; private set; }

            public static T Load<T>(BinaryReader reader)
                where T : EditLogEntry, new()
            {
                T result = new T();
                result.Read(reader);
                return result;
            }

            #region IWritable Members

            public virtual void Write(BinaryWriter writer)
            {
                writer.Write((int)Mutation);
                writer.Write(Date.Ticks);
                writer.Write(Path);
            }

            public virtual void Read(BinaryReader reader)
            {
                // Mutation is not read from the reader here because it has to be read up front to determine what type of class to create.
                Date = new DateTime(reader.ReadInt64());
                Path = reader.ReadString();
            }

            #endregion
        }

        private sealed class CreateDirectoryEditLogEntry : EditLogEntry
        {
            public CreateDirectoryEditLogEntry()
                : base(FileSystemMutation.CreateDirectory)
            {
            }

            public CreateDirectoryEditLogEntry(DateTime date, string path)
                : base(FileSystemMutation.CreateDirectory, date, path)
            {
            }
        }

        private sealed class CreateFileEditLogEntry : EditLogEntry
        {
            public CreateFileEditLogEntry()
                : base(FileSystemMutation.CreateFile)
            {
            }

            public CreateFileEditLogEntry(DateTime date, string path, int blockSize, int replicationFactor)
                : base(FileSystemMutation.CreateFile, date, path)
            {
                BlockSize = blockSize;
                ReplicationFactor = replicationFactor;
            }

            public int BlockSize { get; private set; }

            public int ReplicationFactor { get; private set; }

            public override void Write(BinaryWriter writer)
            {
                base.Write(writer);
                writer.Write(BlockSize);
                writer.Write(ReplicationFactor);
            }

            public override void Read(BinaryReader reader)
            {
                base.Read(reader);
                BlockSize = reader.ReadInt32();
                ReplicationFactor = reader.ReadInt32();
            }
        }

        private sealed class AppendBlockEditLogEntry : EditLogEntry
        {
            public AppendBlockEditLogEntry()
                : base(FileSystemMutation.AppendBlock)
            {
            }

            public AppendBlockEditLogEntry(DateTime date, string path, Guid blockId)
                : base(FileSystemMutation.AppendBlock, date, path)
            {
                BlockId = blockId;
            }

            public Guid BlockId { get; private set; }

            public override void Write(BinaryWriter writer)
            {
                base.Write(writer);
                writer.Write(BlockId.ToByteArray());
            }

            public override void Read(BinaryReader reader)
            {
                base.Read(reader);
                BlockId = new Guid(reader.ReadBytes(16));
            }
        }

        private sealed class CommitBlockEditLogEntry : EditLogEntry
        {
            public CommitBlockEditLogEntry()
                : base(FileSystemMutation.CommitBlock)
            {
            }

            public CommitBlockEditLogEntry(DateTime date, string path, Guid blockId, int size)
                : base(FileSystemMutation.CommitBlock, date, path)
            {
                BlockId = blockId;
                Size = size;
            }

            public Guid BlockId { get; private set; }

            public int Size { get; private set; }

            public override void Write(BinaryWriter writer)
            {
                base.Write(writer);
                writer.Write(BlockId.ToByteArray());
                writer.Write(Size);
            }

            public override void Read(BinaryReader reader)
            {
                base.Read(reader);
                BlockId = new Guid(reader.ReadBytes(16));
                Size = reader.ReadInt32();
            }
        }

        private sealed class CommitFileEditLogEntry : EditLogEntry
        {
            public CommitFileEditLogEntry()
                : base(FileSystemMutation.CommitFile)
            {
            }

            public CommitFileEditLogEntry(DateTime date, string path)
                : base(FileSystemMutation.CommitFile, date, path)
            {
            }
        }

        private sealed class DeleteEditLogEntry : EditLogEntry
        {
            public DeleteEditLogEntry()
                : base(FileSystemMutation.Delete)
            {
            }

            public DeleteEditLogEntry(DateTime date, string path, bool recursive)
                : base(FileSystemMutation.Delete, date, path)
            {
                IsRecursive = recursive;
            }

            public bool IsRecursive { get; private set; }

            public override void Write(BinaryWriter writer)
            {
                base.Write(writer);
                writer.Write(IsRecursive);
            }

            public override void Read(BinaryReader reader)
            {
                base.Read(reader);
                IsRecursive = reader.ReadBoolean();
            }
        }

        private sealed class MoveEditLogEntry : EditLogEntry
        {
            public MoveEditLogEntry()
                : base(FileSystemMutation.Move)
            {
            }

            public MoveEditLogEntry(DateTime date, string path, string targetPath)
                : base(FileSystemMutation.Move, date, path)
            {
                TargetPath = targetPath;
            }

            public string TargetPath { get; private set; }

            public override void Write(BinaryWriter writer)
            {
                base.Write(writer);
                writer.Write(TargetPath);
            }

            public override void Read(BinaryReader reader)
            {
                base.Read(reader);
                TargetPath = reader.ReadString();
            }
        }

        #endregion

        private const string _logFileName = "EditLog";
        private const string _newLogFileName = "EditLog.new";
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(EditLog));
        private readonly object _logFileLock = new object();
        private bool _loggingEnabled = true;
        private readonly string _logFileDirectory;
        private string _logFilePath;
        private FileStream _logFileStream;
        private BinaryWriter _logFileWriter;

        public EditLog(string logFileDirectory)
        {
            if( logFileDirectory == null )
                logFileDirectory = string.Empty;
            if( logFileDirectory.Length > 0 )
                System.IO.Directory.CreateDirectory(logFileDirectory);
            _logFileDirectory = logFileDirectory;
            _logFilePath = Path.Combine(logFileDirectory, _logFileName);
        }

        public bool IsUsingNewLogFile
        {
            get { return _logFilePath == Path.Combine(_logFileDirectory, _newLogFileName); }
        }

        public void InitializeFileSystem(bool replayLog, bool readOnly, FileSystem fileSystem)
        {
            if( replayLog && File.Exists(_logFilePath) )
            {
                _log.Info("Replaying log file.");
                ReplayLog(fileSystem);
                // A read only file system is used to create a checkpoint, and doesn't need to read the new log file.
                if( !readOnly )
                {
                    string newLogFilePath = Path.Combine(_logFileDirectory, _newLogFileName);
                    // We don't need to check for this if _logFilePath doesn't exist; if _logFilePath doesn't exist and newLogFilePath does,
                    // it means that there's also a temp image file which the name server will catch while restarting.
                    if( File.Exists(newLogFilePath) )
                    {
                        _logFilePath = newLogFilePath;
                        _log.Info("Replaying new log file.");
                        ReplayLog(fileSystem);
                    }
                }
                _log.Info("Replaying log file finished.");
                _loggingEnabled = !readOnly;
                if( !readOnly )
                    OpenExistingLogFile();
            }
            else
            {
                if( !readOnly )
                    InitializeLogFile();
                _loggingEnabled = !readOnly;
                fileSystem.CreateDirectory("/", DateTime.UtcNow);
            }
        }

        public void LogCreateDirectory(string path, DateTime date)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            LogMutation(new CreateDirectoryEditLogEntry(date, path));
        }

        public void LogCreateFile(string path, DateTime date, int blockSize, int replicationFactor)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            LogMutation(new CreateFileEditLogEntry(date, path, blockSize, replicationFactor));
        }

        public void LogAppendBlock(string path, DateTime date, Guid blockId)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            LogMutation(new AppendBlockEditLogEntry(date, path, blockId));
        }

        public void LogCommitBlock(string path, DateTime date, Guid blockId, int size)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            LogMutation(new CommitBlockEditLogEntry(date, path, blockId, size));
        }

        public void LogCommitFile(string path)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            LogMutation(new CommitFileEditLogEntry(DateTime.UtcNow, path));
        }

        public void LogDelete(string path, bool recursive)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            LogMutation(new DeleteEditLogEntry(DateTime.UtcNow, path, recursive));
        }

        public void LogMove(string from, string to)
        {
            if( from == null )
                throw new ArgumentNullException("from");
            if( to == null )
                throw new ArgumentNullException("to");

            LogMutation(new MoveEditLogEntry(DateTime.UtcNow, from, to));
        }

        public void SwitchToNewLogFile()
        {
            lock( _logFileLock )
            {
                string newLogFileName = Path.Combine(_logFileDirectory, _newLogFileName);
                if( _logFilePath == newLogFileName )
                    _log.Warn("The edit log was already using the new log file.");
                else
                {
                    _log.Info("Switching to new edit log file.");
                    CloseLogFile();
                    _logFilePath = newLogFileName;
                    InitializeLogFile();
                }
            }
        }

        public void DiscardOldLogFile()
        {
            lock( _logFileLock )
            {
                string newLogFileName = Path.Combine(_logFileDirectory, _newLogFileName);
                string logFileName = Path.Combine(_logFileDirectory, _logFileName);
                if( _logFilePath != newLogFileName )
                    _log.Warn("No old edit log file to discard; no action taken.");
                else
                {
                    _log.Info("Discarding old edit log file, and renaming new log file.");
                    CloseLogFile();

                    File.Delete(logFileName);
                    File.Move(newLogFileName, logFileName);

                    _logFilePath = logFileName;
                    OpenExistingLogFile();
                }
            }
        }

        #region IDisposable Members

        public void Dispose()
        {
            CloseLogFile();
        }

        #endregion

        private void ReplayLog(FileSystem fileSystem)
        {
            try
            {
                _loggingEnabled = false;
                using( FileStream stream = File.OpenRead(_logFilePath) )
                using( BinaryReader reader = new BinaryReader(stream) )
                {
                    int version = reader.ReadInt32();
                    if( version != FileSystem.FileSystemFormatVersion )
                        throw new NotSupportedException("The log file uses an unsupported file system version.");
                    
                    long length = stream.Length;
                    while( stream.Position < length )
                    {
                        FileSystemMutation mutation = (FileSystemMutation)reader.ReadInt32();
                        switch( mutation )
                        {
                        case FileSystemMutation.CreateDirectory:
                            CreateDirectoryEditLogEntry createDirectoryEntry = EditLogEntry.Load<CreateDirectoryEditLogEntry>(reader);
                            fileSystem.CreateDirectory(createDirectoryEntry.Path, createDirectoryEntry.Date);
                            break;
                        case FileSystemMutation.CreateFile:
                            CreateFileEditLogEntry createFileEntry = EditLogEntry.Load<CreateFileEditLogEntry>(reader);
                            fileSystem.CreateFile(createFileEntry.Path, createFileEntry.Date, createFileEntry.BlockSize, createFileEntry.ReplicationFactor, false);
                            break;
                        case FileSystemMutation.AppendBlock:
                            AppendBlockEditLogEntry appendBlockEntry = EditLogEntry.Load<AppendBlockEditLogEntry>(reader);
                            fileSystem.AppendBlock(appendBlockEntry.Path, appendBlockEntry.BlockId, -1);
                            break;
                        case FileSystemMutation.CommitBlock:
                            CommitBlockEditLogEntry commitBlockEntry = EditLogEntry.Load<CommitBlockEditLogEntry>(reader);
                            fileSystem.CommitBlock(commitBlockEntry.Path, commitBlockEntry.BlockId, commitBlockEntry.Size);
                            break;
                        case FileSystemMutation.CommitFile:
                            CommitFileEditLogEntry commitFileEntry = EditLogEntry.Load<CommitFileEditLogEntry>(reader);
                            fileSystem.CloseFile(commitFileEntry.Path);
                            break;
                        case FileSystemMutation.Delete:
                            DeleteEditLogEntry deleteEntry = EditLogEntry.Load<DeleteEditLogEntry>(reader);
                            fileSystem.Delete(deleteEntry.Path, deleteEntry.IsRecursive);
                            break;
                        case FileSystemMutation.Move:
                            MoveEditLogEntry moveEntry = EditLogEntry.Load<MoveEditLogEntry>(reader);
                            fileSystem.Move(moveEntry.Path, moveEntry.TargetPath);
                            break;
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

        private void LogMutation(EditLogEntry entry)
        {
            if( _loggingEnabled )
            {
                try
                {
                    lock( _logFileLock )
                    {
                        entry.Write(_logFileWriter);
                        _logFileWriter.Flush();
                    }
                }
                catch( IOException ex )
                {
                    HandleLoggingError(ex);
                    throw;
                }
            }
        }

        private void InitializeLogFile()
        {
            _log.InfoFormat("Initializing new edit log file at '{0}'.", _logFilePath);
            _logFileStream = File.Open(_logFilePath, FileMode.Create, FileAccess.Write, FileShare.None);
            _logFileWriter = new BinaryWriter(_logFileStream);
            _logFileWriter.Write(FileSystem.FileSystemFormatVersion);
            _logFileWriter.Flush();
        }

        private void OpenExistingLogFile()
        {
            _log.InfoFormat("Opening existing edit log file '{0}' for writing.", _logFilePath);
            _logFileStream = File.Open(_logFilePath, FileMode.Append, FileAccess.Write, FileShare.None);
            _logFileWriter = new BinaryWriter(_logFileStream);
        }

        private void CloseLogFile()
        {
            if( _logFileWriter != null )
                ((IDisposable)_logFileWriter).Dispose();
            if( _logFileStream != null )
                _logFileStream.Dispose();
        }
    }
}
