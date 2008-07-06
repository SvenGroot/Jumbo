using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NameServer
{
    /// <summary>
    /// Manages the file system namespace.
    /// </summary>
    class FileSystem
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileSystem));
        private static Directory _root = new Directory(null, string.Empty, DateTime.UtcNow);

        /// <summary>
        /// The character that separates directory names in a path.
        /// </summary>
        public const char DirectorySeparator = '/';

        /// <summary>
        /// Creates a new directory in the file system.
        /// </summary>
        /// <param name="path">The full path of the new directory.</param>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path or contains an empty component.</exception>
        public void CreateDirectory(string path)
        {
            _log.DebugFormat("CreateDirectory: path = \"{0}\"", path);

            if( path == null )
                throw new ArgumentNullException("path");
            if( !path.StartsWith("/") )
                throw new ArgumentException("Path is not an absolute path.", "path");
            
            string[] components = path.Split(DirectorySeparator);

            lock( _root )
            {
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
                        currentDirectory = CreateDirectory(currentDirectory, component);
                    else
                    {
                        currentDirectory = entry as Directory;
                        // There is no need to rollback changes here since no changes can have been made yet if this happens.
                        if( currentDirectory == null )
                            throw ArgumentException("Path contains a file name.", "path");
                    }
                }
            }
        }

        private Directory CreateDirectory(Directory parent, string name)
        {
            _log.InfoFormat("Creating directory \"{0}\" inside \"{1}\"", name, parent.FullPath);
            return new Directory(parent, name, DateTime.UtcNow);
        }
    }
}
