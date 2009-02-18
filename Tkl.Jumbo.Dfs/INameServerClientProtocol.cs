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
        /// <exception cref="System.IO.DirectoryNotFoundException"><paramref name="directory"/> does not exist.</exception>
        BlockAssignment CreateFile(string path);

        /// <summary>
        /// Deletes the specified file or directory.
        /// </summary>
        /// <param name="path">The path of the file or directory to delete.</param>
        /// <param name="recursive"><see langword="true"/> to delete all children if <paramref name="path"/> refers to a directory; otherwise <see langword="false"/>.</param>
        /// <returns><see langword="true"/> if the file was deleted; <see langword="false"/> if it doesn't exist.</returns>
        bool Delete(string path, bool recursive);

        /// <summary>
        /// Moves the specified file or directory.
        /// </summary>
        /// <param name="from">The path of the file or directory to move.</param>
        /// <param name="to">The path to move the entry to.</param>
        void Move(string from, string to);

        /// <summary>
        /// Gets information about a file.
        /// </summary>
        /// <param name="path">The full path of the file.</param>
        /// <returns>A <see cref="File"/> object referring to the file.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="directory"/> is <see langword="null" />, or <paramref name="name"/> is <see langword="null"/> or an empty string..</exception>
        /// <exception cref="ArgumentException"><paramref name="directory"/> is not an absolute path, contains an empty component, contains a file name, or <paramref name="name"/> refers to an existing file or directory.</exception>
        /// <exception cref="System.IO.DirectoryNotFoundException"><paramref name="directory"/> does not exist.</exception>
        File GetFileInfo(string path);

        /// <summary>
        /// Appends a new block to a file.
        /// </summary>
        /// <param name="path">The full path of the file to which to append a block.</param>
        /// <returns>Information about the new block.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="directory"/> is <see langword="null" />, or <paramref name="name"/> is <see langword="null"/> or an empty string..</exception>
        /// <exception cref="ArgumentException"><paramref name="directory"/> is not an absolute path, contains an empty component, contains a file name, or <paramref name="name"/> refers to an existing file or directory.</exception>
        /// <exception cref="System.IO.DirectoryNotFoundException"><paramref name="directory"/> does not exist.</exception>
        BlockAssignment AppendBlock(string path);

        /// <summary>
        /// Closes a file that is open for writing.
        /// </summary>
        /// <param name="path">The path of the file to close.</param>
        void CloseFile(string path);

        /// <summary>
        /// Gets the list of data servers that have the specified block.
        /// </summary>
        /// <param name="blockID">The <see cref="Guid"/> identifying the block.</param>
        /// <returns>A list of <see cref="ServerAddress"/> objects that give the addresses of the servers that have this block.</returns>
        ServerAddress[] GetDataServersForBlock(Guid blockID);

        /// <summary>
        /// Waits until safe mode is off or the time out expires.
        /// </summary>
        /// <param name="timeout">The maximum time to wait for safe mode to be turned off in milliseconds, or <see cref="System.Threading.Timeout.Infinite"/> to wait indefinitely.</param>
        /// <returns><see langword="true"/> if safe mode was turned off; <see langword="false"/> if the time out expired.</returns>
        bool WaitForSafeModeOff(int timeout);

        /// <summary>
        /// Gets current metrics for the distributed file system.
        /// </summary>
        /// <returns>An object holding the metrics for the name server.</returns>
        DfsMetrics GetMetrics();

        /// <summary>
        /// Gets the number of blocks from the specified block list that the data server has.
        /// </summary>
        /// <param name="dataServer">The data server whose blocks to check.</param>
        /// <param name="blocks">The blocks to check for.</param>
        /// <returns>The number of blocks.</returns>
        /// <remarks>
        /// This function returns the number of items in the intersection of <paramref name="blocks"/>
        /// and the block list for the specified server.
        /// </remarks>
        int GetDataServerBlockCount(ServerAddress dataServer, Guid[] blocks);

        /// <summary>
        /// Gets the list of blocks that a particular data server has.
        /// </summary>
        /// <param name="dataServer">The data server whose blocks to return.</param>
        /// <returns>The block IDs of all the blocks on that server.</returns>
        Guid[] GetDataServerBlocks(ServerAddress dataServer);

        /// <summary>
        /// Gets the contents of the diagnostic log file.
        /// </summary>
        /// <param name="maxSize">The maximum number of bytes to return.</param>
        /// <returns>The contents of the diagnostic log file.</returns>
        /// <remarks>
        /// If the log file is larger than <paramref name="maxSize"/>, the tail of the file up to the
        /// specified size is returned.
        /// </remarks>
        string GetLogFileContents(int maxSize);

        /// <summary>
        /// Gets a value that indicates whether safe mode is on or off.
        /// </summary>
        bool SafeMode { get; }

        /// <summary>
        /// Gets the maximum size of a single block in a file.
        /// </summary>
        int BlockSize { get; }
    }
}
