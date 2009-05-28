﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Runtime.Remoting.Messaging;

namespace NameServerApplication
{
    /// <summary>
    /// Manages the file system namespace.
    /// </summary>
    public class FileSystem
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileSystem));
        private readonly DfsDirectory _root = new DfsDirectory(null, string.Empty, DateTime.UtcNow);
        private readonly EditLog _editLog;
        private readonly NameServer _nameServer;
        private readonly Dictionary<string, PendingFile> _pendingFiles = new Dictionary<string, PendingFile>();
        private long _totalSize;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileSystem"/> class.
        /// </summary>
        public FileSystem(NameServer nameServer)
            : this(nameServer, false)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileSystem"/> class.
        /// </summary>
        /// <param name="replayLog"><see langword="true"/> to initialize the file system from an existing log file; <see langword="false" />  to create a new file system.</param>
        public FileSystem(NameServer nameServer, bool replayLog)
        {
            if( nameServer == null )
                throw new ArgumentNullException("nameServer");
            _nameServer = nameServer;
            _log.Info("++++ FileSystem created.");
            _editLog = new EditLog(replayLog, nameServer.Configuration.NameServer.EditLogDirectory);
            if( replayLog )
            {
                _log.Info("Replaying log file.");
                _editLog.ReplayLog(this);
                _log.Info("Replaying log file finished.");
                var pendingFiles = _pendingFiles.Keys.ToArray(); // make a copy
                foreach( var file in pendingFiles )
                {
                    _log.WarnFormat("File {0} was not committed before previous name server shutdown and is still open.", file);
                }
            }
        }

        public long TotalSize
        {
            get { return _totalSize; }
        }

        public NameServer NameServer
        {
            get { return _nameServer; }
        }

        /// <summary>
        /// Creates a new directory in the file system.
        /// </summary>
        /// <param name="path">The full path of the new directory.</param>
        /// <returns>A <see cref="DfsDirectory"/> object representing the newly created directory.</returns>
        /// <remarks>
        /// <para>
        ///   If the directory already existed, no changes are made and the existing directory is returned.
        /// </para>
        /// <para>
        ///   The returned <see cref="DfsDirectory"/> object is a shallow copy and cannot be used to modify the internal
        ///   state of the file system. It contains information only about the direct children of the directory, not any
        ///   further descendants.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        public DfsDirectory CreateDirectory(string path)
        {
            return CreateDirectory(path, DateTime.UtcNow);
        }

        /// <summary>
        /// Creates a new directory in the file system.
        /// </summary>
        /// <param name="path">The full path of the new directory.</param>
        /// <returns>A <see cref="DfsDirectory"/> object representing the newly created directory.</returns>
        /// <remarks>
        /// <para>
        ///   If the directory already existed, no changes are made and the existing directory is returned.
        /// </para>
        /// <para>
        ///   The returned <see cref="DfsDirectory"/> object is a shallow copy and cannot be used to modify the internal
        ///   state of the file system. It contains information only about the direct children of the directory, not any
        ///   further descendants.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        public DfsDirectory CreateDirectory(string path, DateTime dateCreated)
        {
            _log.DebugFormat("CreateDirectory: path = \"{0}\"", path);

            DfsDirectory result = GetDirectoryInternal(path, true, dateCreated);
            if( result != null )
                result = (DfsDirectory)result.ShallowClone();
            return result;
        }

        /// <summary>
        /// Gets information about a directory in the file system.
        /// </summary>
        /// <param name="path">The full path of the directory.</param>
        /// <returns>A <see cref="DfsDirectory"/> object representing the directory.</returns>
        /// <remarks>
        ///   The returned <see cref="DfsDirectory"/> object is a shallow copy and cannot be used to modify the internal
        ///   state of the file system. It contains information only about the direct children of the directory, not any
        ///   further descendants.
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        public DfsDirectory GetDirectoryInfo(string path)
        {
            _log.DebugFormat("GetDirectory: path = \"{0}\"", path);

            DfsDirectory result = GetDirectoryInternal(path, false, DateTime.Now);
            if( result != null )
                result = (DfsDirectory)result.ShallowClone();
            return result;
        }

        /// <summary>
        /// Creates a new file in the specified directory.
        /// </summary>
        /// <param name="path">The full path of the new file.</param>
        /// <returns>The block ID of the first block of the new file.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="directory"/> is <see langword="null" />, or <paramref name="name"/> is <see langword="null"/> or an empty string..</exception>
        /// <exception cref="ArgumentException"><paramref name="directory"/> is not an absolute path, contains an empty component, contains a file name, or <paramref name="name"/> refers to an existing file or directory.</exception>
        /// <exception cref="DirectoryNotFoundException"><paramref name="directory"/> does not exist.</exception>
        public Guid CreateFile(string path)
        {
            return CreateFile(path, DateTime.UtcNow, true).Value;
        }

        /// <summary>
        /// Creates a new file in the specified directory.
        /// </summary>
        /// <param name="path">The full path of the new file.</param>
        /// <returns>The block ID of the first block of the new file.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="directory"/> is <see langword="null" />, or <paramref name="name"/> is <see langword="null"/> or an empty string..</exception>
        /// <exception cref="ArgumentException"><paramref name="directory"/> is not an absolute path, contains an empty component, contains a file name, or <paramref name="name"/> refers to an existing file or directory.</exception>
        /// <exception cref="DirectoryNotFoundException"><paramref name="directory"/> does not exist.</exception>
        public Guid? CreateFile(string path, DateTime dateCreated, bool appendBlock)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            _log.DebugFormat("CreateFile: path = \"{0}\"", path);

            lock( _root )
            {
                string name;
                DfsDirectory parent;
                FileSystemEntry entry;
                FindEntry(path, out name, out parent, out entry);
                if( entry != null )
                    throw new ArgumentException("The specified directory already has a file or directory with the specified name.", "name");
                
                PendingFile file = CreateFile(parent, name, dateCreated);
                try
                {
                    if( appendBlock )
                    {
                        Guid blockID = NewBlockID();
                        AppendBlock(file, blockID, true);
                        return blockID;
                    }
                    else
                        return null;
                }
                catch( Exception )
                {
                    CloseFile(path);
                    Delete(path, false);
                    throw;
                }
            }
        }

        /// <summary>
        /// Gets information about a file.
        /// </summary>
        /// <param name="path">The full path of the file.</param>
        /// <returns>A <see cref="DfsFile"/> object referring to the file.</returns>
        /// <remarks>
        ///   The returned <see cref="DfsFile"/> object is a shallow copy and cannot be used to modify the internal
        ///   state of the file system.
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        /// <exception cref="System.IO.DirectoryNotFoundException">One of the parent directories in the path specified in <paramref name="path"/> does not exist.</exception>
        public DfsFile GetFileInfo(string path)
        {
            if( path == null )
                throw new ArgumentNullException("name");

            _log.DebugFormat("GetFileInfo: path = \"{0}\"", path);

            DfsFile result = null;
            lock( _root )
            {
                result = GetFileInfoInternal(path);
                if( result != null )
                    result = (DfsFile)result.ShallowClone();
            }
            _log.Debug("GetFileInfo complete.");
            return result;
        }

        /// <summary>
        /// Gets information about a file or directory.
        /// </summary>
        /// <param name="path">The full path of the file or directory.</param>
        /// <returns>A <see cref="FileSystemEntry"/> object referring to the file or directory.</returns>
        /// <remarks>
        ///   The returned <see cref="FileSystemEntry"/> object is a shallow copy and cannot be used to modify the internal
        ///   state of the file system.
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        /// <exception cref="System.IO.DirectoryNotFoundException">One of the parent directories in the path specified in <paramref name="path"/> does not exist.</exception>
        public FileSystemEntry GetFileSystemEntryInfo(string path)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            _log.DebugFormat("GetFileSystemEntryInfo: path = \"{0}\"", path);

            FileSystemEntry result;
            lock( _root )
            {
                string name;
                DfsDirectory parent;
                FindEntry(path, out name, out parent, out result);
                if( result != null )
                    result = result.ShallowClone();
            }
            return result;
        }

        /// <summary>
        /// Appends a block to an existing file.
        /// </summary>
        /// <param name="path">The path of the file to append the block to.</param>
        /// <returns>The block ID of the the new block.</returns>
        /// <remarks>
        /// The file must be open for writing.
        /// </remarks>
        public Guid AppendBlock(string path)
        {
            _log.DebugFormat("AppendBlock: path = \"{0}\"", path);
            Guid blockID = NewBlockID();
            AppendBlock(path, blockID, true);
            return blockID;
        }

        /// <summary>
        /// Appens the specified block to a file. This function is meant for log file replaying.
        /// </summary>
        /// <param name="path">The path of the file to append the block to.</param>
        /// <param name="blockID">The ID of the block to append.</param>
        /// <param name="checkReplication"><see langword="true"/> to check all existing blocks for replication before appending the new block; <see langword="false"/> to skip the replication check.</param>
        public void AppendBlock(string path, Guid blockID, bool checkReplication)
        {
            // checkReplication is provided so we can skip that while replaying the log file.

            lock( _root )
            {
                PendingFile file;
                if( !(_pendingFiles.TryGetValue(path, out file) && file.File.IsOpenForWriting) )
                    throw new InvalidOperationException(string.Format("The file '{0}' does not exist or is not open for writing.", path));

                AppendBlock(file, blockID, checkReplication);
            }
        }

        /// <summary>
        /// Commit a pending block and add it to the list of blocks for that file.
        /// </summary>
        /// <param name="path">The file whose block to commit.</param>
        /// <param name="blockID">The ID of the block to commit.</param>
        /// <param name="size">The size of the committed block. This is used to update the size of the file.</param>
        public void CommitBlock(string path, Guid blockID, int size)
        {
            _log.DebugFormat("CommitBlock: path = \"{0}\", blockID = {1}, size = {2}", path, blockID, size);
            lock( _root )
            {
                PendingFile file;
                if( !_pendingFiles.TryGetValue(path, out file) )
                    throw new InvalidOperationException(string.Format("The file '{0}' does not exist or is not open for writing.", path));

                if( file.PendingBlock == null || file.PendingBlock != blockID )
                    throw new InvalidOperationException("No block to commit.");

                _editLog.LogCommitBlock(path, DateTime.UtcNow, blockID, size);
                file.File.Blocks.Add(file.PendingBlock.Value);
                file.File.Size += size;
                _totalSize += size;
                file.PendingBlock = null;
                if( !file.File.IsOpenForWriting )
                {
                    _log.DebugFormat("File {0} is no longer pending.", path);
                    lock( _pendingFiles )
                    {
                        _pendingFiles.Remove(path);
                    }
                }
            }
            _nameServer.CommitBlock(blockID);
        }

        /// <summary>
        /// Closes a file that is open for writing.
        /// </summary>
        /// <param name="path">The path of the file to close.</param>
        public void CloseFile(string path)
        {
            CloseFile(path, true);
        }

        /// <summary>
        /// Closes a file that is open for writing.
        /// </summary>
        /// <param name="path">The path of the file to close.</param>
        /// <param name="discardPendingBlocks"><see langword="true"/> to discard pending blocks.</param>
        public void CloseFile(string path, bool discardPendingBlocks)
        {
            // TODO: Once we have leases and stuff, only the client holding the file open may do this.
            _log.DebugFormat("CloseFile: path = \"{0}\"", path);
            lock( _root )
            {
                PendingFile file;
                if( !(_pendingFiles.TryGetValue(path, out file) && file.File.IsOpenForWriting) )
                    throw new InvalidOperationException(string.Format("The file '{0}' does not exist or is not open for writing.", path));

                if( file.PendingBlock != null )
                {
                    if( discardPendingBlocks )
                    {
                        _nameServer.DiscardBlock(file.PendingBlock.Value);
                        file.PendingBlock = null;
                    }
                    else
                        throw new InvalidOperationException(string.Format("The file '{0}' cannot be closed because it has pending block {1}.", path, file.PendingBlock.Value));
                }

                _log.InfoFormat("Closing file {0}", path);
                _editLog.LogCommitFile(path, discardPendingBlocks);
                file.File.IsOpenForWriting = false;
                lock( _pendingFiles )
                {
                    _pendingFiles.Remove(path);
                }
            }
        }

        /// <summary>
        /// Deletes the specified file or directory.
        /// </summary>
        /// <param name="path">The path of the file or directory to delete.</param>
        /// <param name="recursive"><see langword="true"/> to delete all children if <paramref name="path"/> refers to a directory; otherwise <see langword="false"/>.</param>
        /// <returns><see langword="true"/> if the file was deleted; <see langword="false"/> if it doesn't exist.</returns>
        public bool Delete(string path, bool recursive)
        {
            _log.DebugFormat("Delete: path = \"{0}\", recursive = {1}", path, recursive);
            string name;
            DfsDirectory parent;
            FileSystemEntry entry;
            // The entire operation must be locked, otherwise it opens up the possibility of someone else deleting
            // the file.
            lock( _root )
            {
                try
                {
                    FindEntry(path, out name, out parent, out entry);
                }
                catch( System.IO.DirectoryNotFoundException )
                {
                    return false;
                }

                if( entry == null )
                    return false;

                DfsDirectory dir = entry as DfsDirectory;
                if( dir != null && dir.Children.Count > 0 && !recursive )
                    throw new InvalidOperationException("The specified directory is not empty.");

                DeleteInternal(parent, entry, recursive);
                return true;
            }
        }

        public void Move(string from, string to)
        {
            if( from == null )
                throw new ArgumentNullException("from");
            if( to == null )
                throw new ArgumentNullException("to");
            _log.DebugFormat("Move: from = \"{0}\", to = \"{1}\"", from, to);
            lock( _root )
            {
                string fromName;
                FileSystemEntry fromEntry;
                DfsDirectory fromParent;
                FindEntry(from, out fromName, out fromParent, out fromEntry);

                if( fromEntry == null )
                    throw new ArgumentException("The file or directory \"{0}\" does not exist.", from);

                string toName;
                FileSystemEntry toEntry;
                DfsDirectory toParent;
                FindEntry(to, out toName, out toParent, out toEntry);
                if( toEntry is DfsDirectory )
                {
                    toName = null;
                    toParent = (DfsDirectory)toEntry;
                }
                else if( toEntry != null )
                    throw new ArgumentException(string.Format("The path \"{0}\" is an existing file."));

                Move(fromEntry, toParent, toName);
            }
        }

        private Guid NewBlockID()
        {
            return Guid.NewGuid();
        }

        private void AppendBlock(PendingFile file, Guid blockID, bool checkReplication)
        {
            if( file.PendingBlock != null )
                throw new InvalidOperationException("Cannot add a block to a file with a pending block.");

            if( file.File.Size % NameServer.BlockSize != 0 )
                throw new InvalidOperationException("The final block of the file is smaller than the maximum block size, therefore the file can no longer be extended.");

            if( checkReplication )
                NameServer.CheckBlockReplication(file.File.Blocks);

            _log.InfoFormat("Appending new block {0} to file {1}", blockID, file.File.FullPath);
            _editLog.LogAppendBlock(file.File.FullPath, DateTime.UtcNow, blockID);
            file.PendingBlock = blockID;
            NameServer.NotifyNewBlock(file.File, blockID);
        }

        private DfsFile GetFileInfoInternal(string path)
        {
            string name;
            DfsDirectory parent;
            DfsFile result;
            FindFile(path, out name, out parent, out result);
            return result;
        }

        private void FindFile(string path, out string name, out DfsDirectory parent, out DfsFile file)
        {
            FileSystemEntry entry;
            FindEntry(path, out name, out parent, out entry);
            file = entry as DfsFile;
        }

        /// <summary>
        /// Note: This function must be called with _root already locked.
        /// </summary>
        private void FindEntry(string path, out string name, out DfsDirectory parent, out FileSystemEntry file)
        {
            string directory;

            ExtractDirectoryAndFileName(path, out directory, out name);
            if( string.IsNullOrEmpty(name) )
                throw new ArgumentException("No file name specified.");

            parent = GetDirectoryInternal(directory, false, DateTime.Now);
            if( parent == null )
                throw new System.IO.DirectoryNotFoundException("The specified directory does not exist.");

            file = FindEntry(parent, name);
        }
        
        private FileSystemEntry FindEntry(DfsDirectory parent, string name)
        {
            return (from child in parent.Children
                    where child.Name == name
                    select child).FirstOrDefault();
        }

        private static void ExtractDirectoryAndFileName(string path, out string directory, out string name)
        {
            int index = path.LastIndexOf(DfsPath.DirectorySeparator);
            if( index == -1 )
                throw new ArgumentException("Path is not rooted.", "path");
            directory = path.Substring(0, index);
            name = path.Substring(index + 1);
            if( directory.Length == 0 )
                directory = "/";
        }

        private DfsDirectory GetDirectoryInternal(string path, bool create, DateTime creationDate)
        {
            if( path == null )
                throw new ArgumentNullException("path");
            if( !path.StartsWith("/") )
                throw new ArgumentException("Path is not an absolute path.", "path");

            string[] components = path.Split(DfsPath.DirectorySeparator);

            lock( _root )
            {
                if( path == "/" )
                    return _root;

                // First check for empty components so we don't have to roll back changes if there are any.
                // Count must be 1 because the first component will always be empty.
                if( (from c in components where c.Length == 0 select c).Count() > 1 )
                    throw new ArgumentException("Path contains an empty components.", "path");

                DfsDirectory currentDirectory = _root;
                for( int x = 1; x < components.Length; ++x )
                {
                    string component = components[x];
                    var entry = (from e in currentDirectory.Children
                                 where e.Name == component
                                 select e).FirstOrDefault();
                    if( entry == null )
                    {
                        if( create )
                            currentDirectory = CreateDirectory(currentDirectory, component, creationDate);
                        else
                            return null;
                    }
                    else
                    {
                        currentDirectory = entry as DfsDirectory;
                        // There is no need to rollback changes here since no changes can have been made yet if this happens.
                        if( currentDirectory == null )
                            throw new ArgumentException("Path contains a file name.", "path");
                    }
                }
                return currentDirectory;
            }
        }

        private DfsDirectory CreateDirectory(DfsDirectory parent, string name, DateTime dateCreated)
        {
            _log.InfoFormat("Creating directory \"{0}\" inside \"{1}\"", name, parent.FullPath);
            _editLog.LogCreateDirectory(AppendPath(parent.FullPath, name), dateCreated);
            return new DfsDirectory(parent, name, dateCreated);
        }

        private PendingFile CreateFile(DfsDirectory parent, string name, DateTime dateCreated)
        {
            _log.InfoFormat("Creating file \"{0}\" inside \"{1}\"", name, parent.FullPath);
            _editLog.LogCreateFile(AppendPath(parent.FullPath, name), dateCreated);
            PendingFile result = new PendingFile(new DfsFile(parent, name, dateCreated) { IsOpenForWriting = true });
            lock( _pendingFiles )
            {
                _pendingFiles.Add(result.File.FullPath, result);
            }
            return result;
        }

        private void DeleteInternal(DfsDirectory parent, FileSystemEntry entry, bool recursive)
        {
            _log.InfoFormat("Deleting file system entry \"{0}\"", entry.FullPath);
            _editLog.LogDelete(entry.FullPath, recursive);
            parent.Children.Remove(entry);
            DfsFile file = entry as DfsFile;
            if( file != null )
            {
                DeleteFile(file);
            }
            else if( recursive )
            {
                // We've already established the entry is not a File, so it has to be a Directory
                DeleteFilesRecursive((DfsDirectory)entry);
            }
        }

        private void Move(FileSystemEntry entry, DfsDirectory newParent, string newName)
        {
            string to = DfsPath.Combine(newParent.FullPath, newName ?? entry.Name);
            _log.InfoFormat("Moving file system entry \"{0}\" to \"{1}\".", entry.FullPath, to);
            _editLog.LogMove(entry.FullPath, to);
            entry.MoveTo(newParent, newName);
        }


        private void DeleteFilesRecursive(DfsDirectory dir)
        {
            foreach( FileSystemEntry entry in dir.Children )
            {
                DfsDirectory childDir = entry as DfsDirectory;
                if( childDir != null )
                    DeleteFilesRecursive(childDir);
                else
                    DeleteFile((DfsFile)entry);
            }
        }

        private void DeleteFile(DfsFile file)
        {
            _log.InfoFormat("Deleting blocks associated with file {0}.", file.FullPath);
            Guid? pendingBlock = null;
            if( file.IsOpenForWriting )
            {
                _log.WarnFormat("Deleted file {0} was open for writing.", file.FullPath);
                lock( _pendingFiles )
                {
                    PendingFile pendingFile = _pendingFiles[file.FullPath];
                    pendingBlock = pendingFile.PendingBlock;
                    _pendingFiles.Remove(file.FullPath);
                }
            }
            _totalSize -= file.Size; // inside _root lock so safe.
            _nameServer.RemoveFileBlocks(file, pendingBlock);
        }

        private string AppendPath(string parent, string child)
        {
            return DfsPath.Combine(parent, child);
        }
    }
}
