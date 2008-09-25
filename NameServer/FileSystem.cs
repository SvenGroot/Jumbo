using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Runtime.Remoting.Messaging;

namespace NameServer
{
    /// <summary>
    /// Manages the file system namespace.
    /// </summary>
    class FileSystem
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileSystem));
        private Directory _root = new Directory(null, string.Empty, DateTime.UtcNow);
        private EditLog _editLog;
        private NameServer _nameServer;

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
            _editLog = new EditLog(replayLog);
            if( replayLog )
            {
                _log.Info("Replaying log file.");
                _editLog.ReplayLog(this);
                _log.Info("Replaying log file finished.");
            }
        }

        /// <summary>
        /// Creates a new directory in the file system.
        /// </summary>
        /// <param name="path">The full path of the new directory.</param>
        /// <returns>A <see cref="Directory"/> object representing the newly created directory.</returns>
        /// <remarks>
        /// <para>
        ///   If the directory already existed, no changes are made and the existing directory is returned.
        /// </para>
        /// <para>
        ///   The returned <see cref="Directory"/> object is a shallow copy and cannot be used to modify the internal
        ///   state of the file system. It contains information only about the direct children of the directory, not any
        ///   further descendants.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        public Directory CreateDirectory(string path)
        {
            return CreateDirectory(path, DateTime.UtcNow);
        }

        /// <summary>
        /// Creates a new directory in the file system.
        /// </summary>
        /// <param name="path">The full path of the new directory.</param>
        /// <returns>A <see cref="Directory"/> object representing the newly created directory.</returns>
        /// <remarks>
        /// <para>
        ///   If the directory already existed, no changes are made and the existing directory is returned.
        /// </para>
        /// <para>
        ///   The returned <see cref="Directory"/> object is a shallow copy and cannot be used to modify the internal
        ///   state of the file system. It contains information only about the direct children of the directory, not any
        ///   further descendants.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        public Directory CreateDirectory(string path, DateTime dateCreated)
        {
            _log.DebugFormat("CreateDirectory: path = \"{0}\"", path);

            Directory result = GetDirectoryInternal(path, true, dateCreated);
            if( result != null )
                result = (Directory)result.ShallowClone();
            return result;
        }

        /// <summary>
        /// Gets information about a directory in the file system.
        /// </summary>
        /// <param name="path">The full path of the directory.</param>
        /// <returns>A <see cref="Directory"/> object representing the directory.</returns>
        /// <remarks>
        ///   The returned <see cref="Directory"/> object is a shallow copy and cannot be used to modify the internal
        ///   state of the file system. It contains information only about the direct children of the directory, not any
        ///   further descendants.
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        public Directory GetDirectoryInfo(string path)
        {
            _log.DebugFormat("GetDirectory: path = \"{0}\"", path);

            Directory result = GetDirectoryInternal(path, false, DateTime.Now);
            if( result != null )
                result = (Directory)result.ShallowClone();
            return result;
        }

        /// <summary>
        /// Creates a new file in the specified directory.
        /// </summary>
        /// <param name="path">The full path of the new file.</param>
        /// <returns>A <see cref="File"/> object referring to the new file.</returns>
        /// <remarks>
        ///   The returned <see cref="File"/> object is a shallow copy and cannot be used to modify the internal
        ///   state of the file system.
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="directory"/> is <see langword="null" />, or <paramref name="name"/> is <see langword="null"/> or an empty string..</exception>
        /// <exception cref="ArgumentException"><paramref name="directory"/> is not an absolute path, contains an empty component, contains a file name, or <paramref name="name"/> refers to an existing file or directory.</exception>
        /// <exception cref="DirectoryNotFoundException"><paramref name="directory"/> does not exist.</exception>
        public File CreateFile(string path)
        {
            return CreateFile(path, DateTime.UtcNow);
        }

        /// <summary>
        /// Creates a new file in the specified directory.
        /// </summary>
        /// <param name="path">The full path of the new file.</param>
        /// <returns>A <see cref="File"/> object referring to the new file.</returns>
        /// <remarks>
        ///   The returned <see cref="File"/> object is a shallow copy and cannot be used to modify the internal
        ///   state of the file system.
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="directory"/> is <see langword="null" />, or <paramref name="name"/> is <see langword="null"/> or an empty string..</exception>
        /// <exception cref="ArgumentException"><paramref name="directory"/> is not an absolute path, contains an empty component, contains a file name, or <paramref name="name"/> refers to an existing file or directory.</exception>
        /// <exception cref="DirectoryNotFoundException"><paramref name="directory"/> does not exist.</exception>
        public File CreateFile(string path, DateTime dateCreated)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            _log.DebugFormat("CreateFile: path = \"{0}\"", path);

            lock( _root )
            {
                string name;
                Directory parent;
                FileSystemEntry entry;
                FindEntry(path, out name, out parent, out entry);
                if( entry != null )
                    throw new ArgumentException("The specified directory already has a file or directory with the specified name.", "name");
                
                return (File)CreateFile(parent, name, dateCreated).ShallowClone();
            }
        }

        /// <summary>
        /// Gets information about a file.
        /// </summary>
        /// <param name="path">The full path of the file.</param>
        /// <returns>A <see cref="File"/> object referring to the file.</returns>
        /// <remarks>
        ///   The returned <see cref="File"/> object is a shallow copy and cannot be used to modify the internal
        ///   state of the file system.
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="directory"/> is <see langword="null" />, or <paramref name="name"/> is <see langword="null"/> or an empty string..</exception>
        /// <exception cref="ArgumentException"><paramref name="directory"/> is not an absolute path, contains an empty component, contains a file name, or <paramref name="name"/> refers to an existing file or directory.</exception>
        /// <exception cref="DirectoryNotFoundException"><paramref name="directory"/> does not exist.</exception>
        public File GetFileInfo(string path)
        {
            if( path == null )
                throw new ArgumentNullException("name");

            _log.DebugFormat("GetFileInfo: path = \"{0}\"", path);

            lock( _root )
            {
                File result = GetFileInfoInternal(path);
                if( result != null )
                    return (File)result.ShallowClone();
                return null;
            }
        }

        public Guid AppendBlock(string path)
        {
            lock( _root )
            {
                File file = GetFileInfoInternal(path);
                if( file == null )
                    throw new System.IO.FileNotFoundException(string.Format("The file '{0}' does not exist.", path));
                if( file.IsOpenForWriting )
                    throw new InvalidOperationException(string.Format("The file '{0}' is not open for writing.", path));

                _nameServer.CheckBlockReplication(file.Blocks);

                Guid blockID = Guid.NewGuid();
                file.Blocks.Add(blockID);
                return blockID;
            }

        }

        private File GetFileInfoInternal(string path)
        {
            string name;
            Directory parent;
            File result;
            FindFile(path, out name, out parent, out result);
            return result;
        }

        private void FindFile(string path, out string name, out Directory parent, out File file)
        {
            FileSystemEntry entry;
            FindEntry(path, out name, out parent, out entry);
            file = entry as File;
        }

        private void FindEntry(string path, out string name, out Directory parent, out FileSystemEntry file)
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
        
        private FileSystemEntry FindEntry(Directory parent, string name)
        {
            return (from child in parent.Children
                    where child.Name == name
                    select child).FirstOrDefault();
        }

        private static void ExtractDirectoryAndFileName(string path, out string directory, out string name)
        {
            int index = path.LastIndexOf(FileSystemEntry.DirectorySeparator);
            if( index == -1 )
                throw new ArgumentException("Path is not rooted.", "path");
            directory = path.Substring(0, index);
            name = path.Substring(index + 1);
            if( directory.Length == 0 )
                directory = "/";
        }

        private Directory GetDirectoryInternal(string path, bool create, DateTime creationDate)
        {
            if( path == null )
                throw new ArgumentNullException("path");
            if( !path.StartsWith("/") )
                throw new ArgumentException("Path is not an absolute path.", "path");

            string[] components = path.Split(FileSystemEntry.DirectorySeparator);

            lock( _root )
            {
                if( path == "/" )
                    return _root;

                // First check for empty components so we don't have to roll back changes if there are any.
                // Count must be 1 because the first component will always be empty.
                if( (from c in components where c.Length == 0 select c).Count() > 1 )
                    throw new ArgumentException("Path contains an empty components.", "path");

                Directory currentDirectory = _root;
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
                        currentDirectory = entry as Directory;
                        // There is no need to rollback changes here since no changes can have been made yet if this happens.
                        if( currentDirectory == null )
                            throw new ArgumentException("Path contains a file name.", "path");
                    }
                }
                return currentDirectory;
            }
        }

        private Directory CreateDirectory(Directory parent, string name, DateTime dateCreated)
        {
            _log.InfoFormat("Creating directory \"{0}\" inside \"{1}\"", name, parent.FullPath);
            _editLog.LogMutation(FileSystemMutation.CreateDirectory, AppendPath(parent.FullPath, name), dateCreated);
            return new Directory(parent, name, dateCreated);
        }

        private File CreateFile(Directory parent, string name, DateTime dateCreated)
        {
            _log.InfoFormat("Creating file \"{0}\" inside \"{1}\"", name, parent.FullPath);
            _editLog.LogMutation(FileSystemMutation.CreateFile, AppendPath(parent.FullPath, name), dateCreated);
            return new File(parent, name, dateCreated);
        }

        private string AppendPath(string parent, string child)
        {
            string result = parent;
            if( !parent.EndsWith(FileSystemEntry.DirectorySeparator.ToString()) )
                result += FileSystemEntry.DirectorySeparator;
            return result + child;
        }
    }
}
