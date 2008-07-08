using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NameServer
{
    /// <summary>
    /// Manages the file system namespace.
    /// </summary>
    class FileSystem : IDisposable
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileSystem));
        private Directory _root = new Directory(null, string.Empty, DateTime.UtcNow);
        private EditLog _editLog;

        /// <summary>
        /// The character that separates directory names in a path.
        /// </summary>
        public const char DirectorySeparator = '/';

        /// <summary>
        /// Initializes a new instance of the <see cref="FileSystem"/> class.
        /// </summary>
        public FileSystem()
            : this(false)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileSystem"/> class.
        /// </summary>
        /// <param name="replayLog"><see langword="true"/> to initialize the file system from an existing log file; <see langword="false" />  to create a new file system.</param>
        public FileSystem(bool replayLog)
        {
            _editLog = new EditLog(this, replayLog);
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
            _log.DebugFormat("CreateDirectory: path = \"{0}\"", path);

            Directory result = GetDirectoryInternal(path, true);
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

            Directory result = GetDirectoryInternal(path, false);
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
            if( path == null )
                throw new ArgumentNullException("path");

            _log.DebugFormat("CreateFile: path = \"{0}\"", path);

            string directory;
            string name;
            ExtractDirectoryAndFileName(path, out directory, out name);
            if( string.IsNullOrEmpty(name) )
                throw new ArgumentException("No file name specified.");

            lock( _root )
            {
                Directory parent = GetDirectoryInternal(directory, false);
                if( parent == null )
                    throw new System.IO.DirectoryNotFoundException("The specified directory does not exist.");

                if( FindEntry(parent, name) != null )
                    throw new ArgumentException("The specified directory already has a file or directory with the specified name.", "name");
                
                return (File)CreateFile(parent, name).ShallowClone();
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

            _log.DebugFormat("CreateFile: path = \"{0}\"", path);

            string directory;
            string name;
            ExtractDirectoryAndFileName(path, out directory, out name);
            if( string.IsNullOrEmpty(name) )
                throw new ArgumentException("No file name specified.");

            lock( _root )
            {
                Directory parent = GetDirectoryInternal(directory, false);
                if( parent == null )
                    throw new System.IO.DirectoryNotFoundException("The specified directory does not exist.");

                File result = FindEntry(parent, name) as File;
                if( result != null )
                    return (File)result.ShallowClone();
                return null;
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if( disposing )
                _editLog.Dispose();
        }

        private FileSystemEntry FindEntry(Directory parent, string name)
        {
            return (from child in parent.Children
                    where child.Name == name
                    select child).FirstOrDefault();
        }

        private static void ExtractDirectoryAndFileName(string path, out string directory, out string name)
        {
            int index = path.LastIndexOf(DirectorySeparator);
            if( index == -1 )
                throw new ArgumentException("Path is not rooted.", "path");
            directory = path.Substring(0, index);
            name = path.Substring(index + 1);
            if( directory.Length == 0 )
                directory = "/";
        }

        private Directory GetDirectoryInternal(string path, bool create)
        {
            if( path == null )
                throw new ArgumentNullException("path");
            if( !path.StartsWith("/") )
                throw new ArgumentException("Path is not an absolute path.", "path");

            string[] components = path.Split(DirectorySeparator);

            lock( _root )
            {
                if( path == "/" )
                    return _root;

                // First check for empty components so we don't have to roll back changes if there are any.
                // Count must be 1 because the firs component will always be empty.
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
                            currentDirectory = CreateDirectory(currentDirectory, component);
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

        private Directory CreateDirectory(Directory parent, string name)
        {
            _log.InfoFormat("Creating directory \"{0}\" inside \"{1}\"", name, parent.FullPath);
            _editLog.LogMutation(FileSystemMutation.CreateDirectory, AppendPath(parent.FullPath, name));
            return new Directory(parent, name, DateTime.UtcNow);
        }

        private File CreateFile(Directory parent, string name)
        {
            _log.InfoFormat("Creating file \"{0}\" inside \"{1}\"", name, parent.FullPath);
            _editLog.LogMutation(FileSystemMutation.CreateFile, AppendPath(parent.FullPath, name));
            return new File(parent, name, DateTime.UtcNow);
        }

        private string AppendPath(string parent, string child)
        {
            string result = parent;
            if( !parent.EndsWith(DirectorySeparator.ToString()) )
                result += DirectorySeparator;
            return result + child;
        }

        #region IDisposable Members

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion
    }
}
