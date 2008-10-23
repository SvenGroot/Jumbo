using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Defines the interface used by clients to communicate with the NameServer.
    /// </summary>
    public interface INameServerClientProtocol
    {
        /// <summary>
        /// Creates the specified directory in the distributed file system.
        /// </summary>
        /// <param name="path">The path of the directory to create.</param>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        void CreateDirectory(string path);

        /// <summary>
        /// Gets information about a directory in the file system.
        /// </summary>
        /// <param name="path">The full path of the directory.</param>
        /// <returns>A <see cref="Directory"/> object representing the directory.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        Directory GetDirectoryInfo(string path);

        /// <summary>
        /// Creates a new file in the specified directory.
        /// </summary>
        /// <param name="path">The full path of the new file.</param>
        /// <exception cref="ArgumentNullException"><paramref name="directory"/> is <see langword="null" />, or <paramref name="name"/> is <see langword="null"/> or an empty string..</exception>
        /// <exception cref="ArgumentException"><paramref name="directory"/> is not an absolute path, contains an empty component, contains a file name, or <paramref name="name"/> refers to an existing file or directory.</exception>
        /// <exception cref="DirectoryNotFoundException"><paramref name="directory"/> does not exist.</exception>
        BlockAssignment CreateFile(string path);

        /// <summary>
        /// Deletes the specified file or directory.
        /// </summary>
        /// <param name="path">The path of the file or directory to delete.</param>
        /// <param name="recursive"><see langword="true"/> to delete all children if <paramref name="path"/> refers to a directory; otherwise <see langword="false"/>.</param>
        /// <returns><see langword="true"/> if the file was deleted; <see langword="false"/> if it doesn't exist.</returns>
        bool Delete(string path, bool recursive);

        /// <summary>
        /// Gets information about a file.
        /// </summary>
        /// <param name="path">The full path of the file.</param>
        /// <returns>A <see cref="File"/> object referring to the file.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="directory"/> is <see langword="null" />, or <paramref name="name"/> is <see langword="null"/> or an empty string..</exception>
        /// <exception cref="ArgumentException"><paramref name="directory"/> is not an absolute path, contains an empty component, contains a file name, or <paramref name="name"/> refers to an existing file or directory.</exception>
        /// <exception cref="DirectoryNotFoundException"><paramref name="directory"/> does not exist.</exception>
        File GetFileInfo(string path);

        /// <summary>
        /// Appends a new block to a file.
        /// </summary>
        /// <param name="path">The full path of the file to which to append a block.</param>
        /// <returns>Information about the new block.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="directory"/> is <see langword="null" />, or <paramref name="name"/> is <see langword="null"/> or an empty string..</exception>
        /// <exception cref="ArgumentException"><paramref name="directory"/> is not an absolute path, contains an empty component, contains a file name, or <paramref name="name"/> refers to an existing file or directory.</exception>
        /// <exception cref="DirectoryNotFoundException"><paramref name="directory"/> does not exist.</exception>
        BlockAssignment AppendBlock(string path);

        /// <summary>
        /// Closes a file that is open for writing.
        /// </summary>
        /// <param name="path">The path of the file to close.</param>
        void CloseFile(string path);

        ServerAddress[] GetDataServersForBlock(Guid blockID);

        /// <summary>
        /// Gets the size of a single block in a file.
        /// </summary>
        int BlockSize { get; }
    }
}
