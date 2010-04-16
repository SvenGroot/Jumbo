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
        /// <returns>A <see cref="DfsDirectory"/> object representing the directory.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        DfsDirectory GetDirectoryInfo(string path);

        /// <summary>
        /// Creates a new file in the specified directory.
        /// </summary>
        /// <param name="path">The full path of the new file.</param>
        /// <param name="blockSize">The size of the blocks in the file, or zero to use the file system default block size.</param>
        /// <param name="replicationFactor">The number of replicas to create of the file's blocks, or zero to use the file system default replication factor.</param>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, contains a file name, or refers to an existing file or directory.</exception>
        /// <exception cref="System.IO.DirectoryNotFoundException">Part of the path specified in <paramref name="path"/> does not exist.</exception>
        BlockAssignment CreateFile(string path,  int blockSize, int replicationFactor);

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
        /// <param name="source">The path of the file or directory to move.</param>
        /// <param name="destination">The path to move the entry to.</param>
        void Move(string source, string destination);

        /// <summary>
        /// Gets information about a file.
        /// </summary>
        /// <param name="path">The full path of the file.</param>
        /// <returns>A <see cref="DfsFile"/> object referring to the file.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        /// <exception cref="System.IO.DirectoryNotFoundException">One of the parent directories in the path specified in <paramref name="path"/> does not exist.</exception>
        DfsFile GetFileInfo(string path);

        /// <summary>
        /// Gets information about a file or directory.
        /// </summary>
        /// <param name="path">The full path of the file or directory.</param>
        /// <returns>A <see cref="FileSystemEntry"/> object referring to the file or directory, or <see langword="null" /> if the .</returns>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        /// <exception cref="System.IO.DirectoryNotFoundException">One of the parent directories in the path specified in <paramref name="path"/> does not exist.</exception>
        FileSystemEntry GetFileSystemEntryInfo(string path);

        /// <summary>
        /// Appends a new block to a file.
        /// </summary>
        /// <param name="path">The full path of the file to which to append a block.</param>
        /// <returns>Information about the new block.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" /> an empty string.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, contains a file name, or refers to an existing file or directory.</exception>
        /// <exception cref="System.IO.DirectoryNotFoundException">Part of <paramref name="path"/> does not exist.</exception>
        BlockAssignment AppendBlock(string path);

        /// <summary>
        /// Closes a file that is open for writing.
        /// </summary>
        /// <param name="path">The path of the file to close.</param>
        void CloseFile(string path);

        /// <summary>
        /// Gets the list of data servers that have the specified block.
        /// </summary>
        /// <param name="blockId">The <see cref="Guid"/> identifying the block.</param>
        /// <returns>A list of <see cref="ServerAddress"/> objects that give the addresses of the servers that have this block.</returns>
        ServerAddress[] GetDataServersForBlock(Guid blockId);

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
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
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
        /// Gets the list of blocks, out of the specified blocks, that a particular data server has.
        /// </summary>
        /// <param name="dataServer">The data server whose blocks to return.</param>
        /// <param name="blocks">The list of blocks to filter by.</param>
        /// <returns>The block IDs of all the blocks on that server.</returns>
        Guid[] GetDataServerBlocks(ServerAddress dataServer, Guid[] blocks);

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
        /// Removes the specified data server from the name server's list of known data servers.
        /// </summary>
        /// <param name="dataServer">The address of the data server to remove.</param>
        /// <remarks>
        /// <para>
        ///   If a data server has been shutdown, and is known not to restart soon, you can use this function to remove it
        ///   immediately rather than waiting for the timeout to expire. The name server will remove all information regarding
        ///   to the data server and force an immediate replication check.
        /// </para>
        /// </remarks>
        void RemoveDataServer(ServerAddress dataServer);

        /// <summary>
        /// Immediately creates a checkpoint of the file system namespace.
        /// </summary>
        void CreateCheckpoint();

        /// <summary>
        /// Gets or sets a value that indicates whether safe mode is on or off.
        /// </summary>
        /// <remarks>
        /// Disabling safe mode before full replication is achieved will cause an immediate replication check.
        /// </remarks>
        bool SafeMode { get; set; }

        /// <summary>
        /// Gets the maximum size of a single block in a file.
        /// </summary>
        int BlockSize { get; }
    }
}
