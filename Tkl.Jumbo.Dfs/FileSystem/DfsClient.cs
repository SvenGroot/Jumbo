﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Dfs.FileSystem;

namespace Tkl.Jumbo.Dfs.FileSystem
{
    /// <summary>
    /// Provides client access to the Distributed File System.
    /// </summary>
    public class DfsClient : FileSystemClient
    {
        private const string _nameServerUrlFormat = "tcp://{0}:{1}/NameServer";

        private readonly INameServerClientProtocol _nameServer;
        private static readonly DfsPathUtility _path = new DfsPathUtility(); // Thread-safe, so static is okay

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline")]
        static DfsClient()
        {
            RpcHelper.RegisterClientChannel();
        }

        internal DfsClient(DfsConfiguration config)
            : base(config)
        {
            _nameServer = CreateNameServerClient(config);
        }

        /// <summary>
        /// Gets the <see cref="INameServerClientProtocol"/> used by this instance to communicate with the name server.
        /// </summary>
        public INameServerClientProtocol NameServer
        {
            get { return _nameServer; }
        }

        /// <summary>
        /// Gets the path utility for this file system.
        /// </summary>
        /// <value>
        /// The <see cref="IFileSystemPathUtility"/> implementation for this file system.
        /// </value>
        public override IFileSystemPathUtility Path
        {
            get { return _path; }
        }

        /// <summary>
        /// Gets the default block size for the file system.
        /// </summary>
        /// <value>
        /// The default block size, or 0 if the file system doesn't support blocks.
        /// </value>
        public override int? DefaultBlockSize
        {
            get { return _nameServer.BlockSize; }
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a name server.
        /// </summary>
        /// <returns>An object implementing <see cref="INameServerClientProtocol"/> that is a proxy class for
        /// communicating with the name server via RPC.</returns>
        public static INameServerClientProtocol CreateNameServerClient()
        {
            return CreateNameServerClient(DfsConfiguration.GetConfiguration());
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a name server using the specified configuration.
        /// </summary>
        /// <param name="config">A <see cref="DfsConfiguration"/> that provides the name server configuration to use.</param>
        /// <returns>An object implementing <see cref="INameServerClientProtocol"/> that is a proxy class for
        /// communicating with the name server via RPC.</returns>
        public static INameServerClientProtocol CreateNameServerClient(DfsConfiguration config)
        {
            if( config == null )
                throw new ArgumentNullException("config");

            return CreateNameServerClientInternal<INameServerClientProtocol>(config.NameServer.HostName, config.NameServer.Port);
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a name server via the heartbeat protocol.
        /// </summary>
        /// <returns>An object implementing <see cref="INameServerHeartbeatProtocol"/> that is a proxy class for
        /// communicating with the name server via RPC.</returns>
        public static INameServerHeartbeatProtocol CreateNameServerHeartbeatClient()
        {
            return CreateNameServerHeartbeatClient(DfsConfiguration.GetConfiguration());
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a name server via the heartbeat protocol
        /// using the specified configuration.
        /// </summary>
        /// <param name="config">A <see cref="DfsConfiguration"/> that provides the name server configuration to use.</param>
        /// <returns>An object implementing <see cref="INameServerHeartbeatProtocol"/> that is a proxy class for
        /// communicating with the name server via RPC.</returns>
        public static INameServerHeartbeatProtocol CreateNameServerHeartbeatClient(DfsConfiguration config)
        {
            if( config == null )
                throw new ArgumentNullException("config");

            return CreateNameServerClientInternal<INameServerHeartbeatProtocol>(config.NameServer.HostName, config.NameServer.Port);
        }

        /// <summary>
        /// Gets the contents of the diagnostic log file of a data server.
        /// </summary>
        /// <param name="address">The <see cref="ServerAddress"/> of the data server.</param>
        /// <param name="kind">The kind of log file.</param>
        /// <param name="maxSize">The maximum size of the log data to return.</param>
        /// <returns>The contents of the log file.</returns>
        public static string GetDataServerLogFileContents(ServerAddress address, LogFileKind kind, int maxSize)
        {
            if( address == null )
                throw new ArgumentNullException("address");

            return GetDataServerLogFileContents(address.HostName, address.Port, kind, maxSize);
        }

        /// <summary>
        /// Gets the contents of the diagnostic log file of a data server.
        /// </summary>
        /// <param name="hostName">The host name of the data server.</param>
        /// <param name="port">The port on which the data server is listening.</param>
        /// <param name="kind">The kind of log file.</param>
        /// <param name="maxSize">The maximum size of the log data to return.</param>
        /// <returns>The contents of the log file.</returns>
        public static string GetDataServerLogFileContents(string hostName, int port, LogFileKind kind, int maxSize)
        {
            if( hostName == null )
                throw new ArgumentNullException("hostName");

            using( TcpClient client = new TcpClient(hostName, port) )
            using( NetworkStream stream = client.GetStream() )
            {
                DataServerClientProtocolHeader header = new DataServerClientProtocolGetLogFileContentsHeader(maxSize) { Kind = kind };
                BinaryFormatter formatter = new BinaryFormatter();
                formatter.Serialize(stream, header);
                using( StreamReader reader = new StreamReader(stream) )
                {
                    return reader.ReadToEnd();
                }
            }
        }

        /// <summary>
        /// Creates the specified directory in the file system.
        /// </summary>
        /// <param name="path">The path of the directory to create.</param>
        public override void CreateDirectory(string path)
        {
            _nameServer.CreateDirectory(path);
        }


        /// <summary>
        /// Gets information about a directory in the file system.
        /// </summary>
        /// <param name="path">The full path of the directory.</param>
        /// <returns>
        /// A <see cref="JumboDirectory"/> object representing the directory, or <see langword="null"/> if the directory doesn't exist.
        /// </returns>
        public override JumboDirectory GetDirectoryInfo(string path)
        {
            return _nameServer.GetDirectoryInfo(path);
        }

        /// <summary>
        /// Gets information about a file.
        /// </summary>
        /// <param name="path">The full path of the file.</param>
        /// <returns>
        /// A <see cref="JumboFile"/> object referring to the file, or <see langword="null"/> if the file doesn't exist.
        /// </returns>
        public override JumboFile GetFileInfo(string path)
        {
            return _nameServer.GetFileInfo(path);
        }

        /// <summary>
        /// Gets information about a file or directory.
        /// </summary>
        /// <param name="path">The full path of the file or directory.</param>
        /// <returns>
        /// A <see cref="JumboFileSystemEntry"/> object referring to the file or directory, or <see langword="null"/> if the entry doesn't exist.
        /// </returns>
        public override JumboFileSystemEntry GetFileSystemEntryInfo(string path)
        {
            return _nameServer.GetFileSystemEntryInfo(path);
        }

        /// <summary>
        /// Opens the specified file on the distributed file system for reading.
        /// </summary>
        /// <param name="path">The path of the file.</param>
        /// <returns>A <see cref="Stream"/> that can be used to read the contents of the file.</returns>
        public override Stream OpenFile(string path)
        {
            return new DfsInputStream(NameServer, path);
        }

        /// <summary>
        /// Creates a new file with the specified path on the distributed file system.
        /// </summary>
        /// <param name="path">The path containing the directory and name of the file to create.</param>
        /// <param name="blockSize">The block size of the new file, or zero to use the file system default block size.</param>
        /// <param name="replicationFactor">The number of replicas to create of the file's blocks, or zero to use the file system default replication factor.</param>
        /// <param name="useLocalReplica"><see langword="true"/> to put the first replica on the node that's creating the file if it's part of the DFS cluster; otherwise, <see langword="false"/>.</param>
        /// <param name="recordOptions">The record options for the file.</param>
        /// <returns>
        /// A <see cref="Stream"/> that can be used to write data to the file.
        /// </returns>
        public override Stream CreateFile(string path, int blockSize, int replicationFactor, bool useLocalReplica, RecordStreamOptions recordOptions)
        {
            return new DfsOutputStream(NameServer, path, blockSize, replicationFactor, useLocalReplica, recordOptions);
        }

        /// <summary>
        /// Deletes the specified file or directory.
        /// </summary>
        /// <param name="path">The path of the file or directory to delete.</param>
        /// <param name="recursive"><see langword="true"/> to delete all children if <paramref name="path"/> refers to a directory; otherwise <see langword="false"/>.</param>
        /// <returns><see langword="true"/> if the file was deleted; <see langword="false"/> if it doesn't exist.</returns>
        public override bool Delete(string path, bool recursive)
        {
            return _nameServer.Delete(path, recursive);
        }

        /// <summary>
        /// Moves the specified file or directory.
        /// </summary>
        /// <param name="source">The path of the file or directory to move.</param>
        /// <param name="destination">The path to move the entry to.</param>
        public override void Move(string source, string destination)
        {
            _nameServer.Move(source, destination);
        }

        private static T CreateNameServerClientInternal<T>(string hostName, int port)
        {
            string url = string.Format(System.Globalization.CultureInfo.InvariantCulture, _nameServerUrlFormat, hostName, port);
            return (T)Activator.GetObject(typeof(T), url);
        }
    }
}