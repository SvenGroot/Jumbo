// $Id$
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

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Provides client access to the Distributed File System.
    /// </summary>
    public class DfsClient
    {
        private const string _nameServerUrlFormat = "tcp://{0}:{1}/NameServer";
        private const int _bufferSize = 4096;

        static DfsClient()
        {
            RpcHelper.RegisterClientChannel();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsClient"/> class.
        /// </summary>
        public DfsClient()
            : this(DfsConfiguration.GetConfiguration())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsClient"/> class using the specified configuration.
        /// </summary>
        /// <param name="config">The configuration to use.</param>
        public DfsClient(DfsConfiguration config)
        {
            NameServer = CreateNameServerClient(config);
            Configuration = config;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsClient"/> class using the specified host name and port.
        /// </summary>
        /// <param name="hostName">The host name of the name server.</param>
        /// <param name="port">The port at which the name server is listening.</param>
        public DfsClient(string hostName, int port)
        {
            if( hostName == null )
                throw new ArgumentNullException("hostName");
            Configuration = new DfsConfiguration();
            Configuration.NameServer.HostName = hostName;
            Configuration.NameServer.Port = port;
            NameServer = CreateNameServerClient(hostName, port);
        }

        /// <summary>
        /// Gets the <see cref="INameServerClientProtocol"/> used by this instance to communicate with the name server.
        /// </summary>
        public INameServerClientProtocol NameServer { get; private set; }

        /// <summary>
        /// Gets the <see cref="DfsConfiguration"/> used to create this instance.
        /// </summary>
        public DfsConfiguration Configuration { get; private set; }

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
        /// Creates a client object that can be used to communicate with a name server.
        /// </summary>
        /// <param name="hostName">The host name of the name server.</param>
        /// <param name="port">The port at which the name server is listening.</param>
        /// <returns>An object implementing <see cref="INameServerClientProtocol"/> that is a proxy class for
        /// communicating with the name server via RPC.</returns>
        public static INameServerClientProtocol CreateNameServerClient(string hostName, int port)
        {
            if( hostName == null )
                throw new ArgumentNullException("hostName");

            return CreateNameServerClientInternal<INameServerClientProtocol>(hostName, port);
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
        /// <param name="maxSize">The maximum size of the log data to return.</param>
        /// <returns>The contents of the log file.</returns>
        public static string GetDataServerLogFileContents(ServerAddress address, int maxSize)
        {
            if( address == null )
                throw new ArgumentNullException("address");

            return GetDataServerLogFileContents(address.HostName, address.Port, maxSize);
        }

        /// <summary>
        /// Gets the contents of the diagnostic log file of a data server.
        /// </summary>
        /// <param name="hostName">The host name of the data server.</param>
        /// <param name="port">The port on which the data server is listening.</param>
        /// <param name="maxSize">The maximum size of the log data to return.</param>
        /// <returns>The contents of the log file.</returns>
        public static string GetDataServerLogFileContents(string hostName, int port, int maxSize)
        {
            if( hostName == null )
                throw new ArgumentNullException("hostName");

            using( TcpClient client = new TcpClient(hostName, port) )
            using( NetworkStream stream = client.GetStream() )
            {
                DataServerClientProtocolHeader header = new DataServerClientProtocolGetLogFileContentsHeader(maxSize);
                BinaryFormatter formatter = new BinaryFormatter();
                formatter.Serialize(stream, header);
                using( StreamReader reader = new StreamReader(stream) )
                {
                    return reader.ReadToEnd();
                }
            }
        }

        /// <summary>
        /// Uploads the contents of the specified stream to the Distributed File System.
        /// </summary>
        /// <param name="stream">The stream with the data to upload.</param>
        /// <param name="dfsPath">The path of the file on the DFS to write the data to.</param>
        public void UploadStream(System.IO.Stream stream, string dfsPath)
        {
            UploadStream(stream, dfsPath, 0, 0, true, null);
        }

        /// <summary>
        /// Uploads the contents of the specified stream to the Distributed File System.
        /// </summary>
        /// <param name="stream">The stream with the data to upload.</param>
        /// <param name="dfsPath">The path of the file on the DFS to write the data to.</param>
        /// <param name="blockSize">The block size of the file, or zero to use the file system default block size.</param>
        /// <param name="replicationFactor">The number of replicas to create of the file's blocks, or zero to use the file system default replication factor.</param>
        /// <param name="useLocalReplica"><see langword="true"/> to put the first replica on the node that's creating the file if it's part of the DFS cluster; otherwise, <see langword="false"/>.</param>
        /// <param name="progressCallback">The <see cref="ProgressCallback"/> that will be called to report progress of the operation. May be <see langword="null"/>.</param>
        public void UploadStream(System.IO.Stream stream, string dfsPath, int blockSize, int replicationFactor, bool useLocalReplica, ProgressCallback progressCallback)
        {
            if( dfsPath == null )
                throw new ArgumentNullException("dfsPath");
            if( stream == null )
                throw new ArgumentNullException("stream");

            using( DfsOutputStream outputStream = new DfsOutputStream(NameServer, dfsPath, blockSize, replicationFactor, useLocalReplica, IO.RecordStreamOptions.None) )
            {
                CopyStream(dfsPath, stream, outputStream, progressCallback);
            }
        }

        /// <summary>
        /// Uploads a file to the Distributed File System.
        /// </summary>
        /// <param name="localPath">The path of the file to upload.</param>
        /// <param name="dfsPath">The path on the DFS to store the file. If this is the name of an existing directory, the file
        /// will be stored in that directory.</param>
        public void UploadFile(string localPath, string dfsPath)
        {
            UploadFile(localPath, dfsPath, 0, 0, true, null);
        }

        /// <summary>
        /// Uploads a file to the Distributed File System.
        /// </summary>
        /// <param name="localPath">The path of the file to upload.</param>
        /// <param name="dfsPath">The path on the DFS to store the file. If this is the name of an existing directory, the file
        /// will be stored in that directory.</param>
        /// <param name="blockSize">The block size of the file, or zero to use the file system default block size.</param>
        /// <param name="replicationFactor">The number of replicas to create of the file's blocks, or zero to use the file system default replication factor.</param>
        /// <param name="useLocalReplica"><see langword="true"/> to put the first replica on the node that's creating the file if it's part of the DFS cluster; otherwise, <see langword="false"/>.</param>
        /// <param name="progressCallback">The <see cref="ProgressCallback"/> that will be called to report progress of the operation. May be <see langword="null"/>.</param>
        public void UploadFile(string localPath, string dfsPath, int blockSize, int replicationFactor, bool useLocalReplica, ProgressCallback progressCallback)
        {
            if( dfsPath == null )
                throw new ArgumentNullException("dfsPath");
            if( localPath == null )
                throw new ArgumentNullException("localPath");
            DfsDirectory dir = NameServer.GetDirectoryInfo(dfsPath);
            if( dir != null )
            {
                string fileName = System.IO.Path.GetFileName(localPath);
                if( !dfsPath.EndsWith(DfsPath.DirectorySeparator.ToString(), StringComparison.Ordinal) )
                    dfsPath += DfsPath.DirectorySeparator;
                dfsPath += fileName;
            }
            using( System.IO.FileStream inputStream = System.IO.File.OpenRead(localPath) )
            {
                UploadStream(inputStream, dfsPath, blockSize, replicationFactor, useLocalReplica, progressCallback);
            }
        }

        /// <summary>
        /// Uploads the files in the specified directory to the DFS.
        /// </summary>
        /// <param name="localPath">The path of the directory on the local file system containing the files to upload.</param>
        /// <param name="dfsPath">The path of the directory on the DFS where the files should be stored. This path must not
        /// refer to an existing directory.</param>
        public void UploadDirectory(string localPath, string dfsPath)
        {
            UploadDirectory(localPath, dfsPath, 0, 0, true, null);
        }

        /// <summary>
        /// Uploads the files in the specified directory to the DFS.
        /// </summary>
        /// <param name="localPath">The path of the directory on the local file system containing the files to upload.</param>
        /// <param name="dfsPath">The path of the directory on the DFS where the files should be stored. This path must not
        /// refer to an existing directory.</param>
        /// <param name="blockSize">The block size of the files in the directory, or zero to use the file system default block size.</param>
        /// <param name="replicationFactor">The number of replicas to create of the file's blocks, or zero to use the file system default replication factor.</param>
        /// <param name="useLocalReplica"><see langword="true"/> to put the first replica on the node that's creating the file if it's part of the DFS cluster; otherwise, <see langword="false"/>.</param>
        /// <param name="progressCallback">The <see cref="ProgressCallback"/> that will be called to report progress of the operation. May be <see langword="null"/>.</param>
        public void UploadDirectory(string localPath, string dfsPath, int blockSize, int replicationFactor, bool useLocalReplica, ProgressCallback progressCallback)
        {
            if( localPath == null )
                throw new ArgumentNullException("localPath");
            if( dfsPath == null )
                throw new ArgumentNullException("dfsPath");

            string[] files = System.IO.Directory.GetFiles(localPath);

            DfsDirectory directory = NameServer.GetDirectoryInfo(dfsPath);
            if( directory != null )
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Directory {0} already exists on the DFS.", dfsPath), "dfsPath");
            NameServer.CreateDirectory(dfsPath);

            foreach( string file in files )
            {
                string targetFile = DfsPath.Combine(dfsPath, System.IO.Path.GetFileName(file));
                UploadFile(file, targetFile, blockSize, replicationFactor, useLocalReplica, progressCallback);
            }
        }

        /// <summary>
        /// Downloads the specified file from the DFS, saving it to the specified stream.
        /// </summary>
        /// <param name="dfsPath">The path of the file on the DFS to download.</param>
        /// <param name="stream">The stream to save the file to.</param>
        public void DownloadStream(string dfsPath, System.IO.Stream stream)
        {
            DownloadStream(dfsPath, stream, null);
        }

        /// <summary>
        /// Downloads the specified file from the DFS, saving it to the specified stream.
        /// </summary>
        /// <param name="dfsPath">The path of the file on the DFS to download.</param>
        /// <param name="stream">The stream to save the file to.</param>
        /// <param name="progressCallback">The <see cref="ProgressCallback"/> that will be called to report progress of the operation. May be <see langword="null"/>.</param>
        public void DownloadStream(string dfsPath, System.IO.Stream stream, ProgressCallback progressCallback)
        {
            if( dfsPath == null )
                throw new ArgumentNullException("dfsPath");
            if( stream == null )
                throw new ArgumentNullException("stream");
            using( DfsInputStream inputStream = new DfsInputStream(NameServer, dfsPath) )
            {
                CopyStream(dfsPath, inputStream, stream, progressCallback);
            }
        }

        /// <summary>
        /// Downloads the specified file from the DFS to the specified local file.
        /// </summary>
        /// <param name="dfsPath">The path of the file on the DFS to download.</param>
        /// <param name="localPath">The path of the file on the local file system to save the file to. If this is the
        /// name of an existing directory, the file will be downloaded to that directory.</param>
        public void DownloadFile(string dfsPath, string localPath)
        {
            DownloadFile(dfsPath, localPath, null);
        }

        /// <summary>
        /// Downloads the specified file from the DFS to the specified local file.
        /// </summary>
        /// <param name="dfsPath">The path of the file on the DFS to download.</param>
        /// <param name="localPath">The path of the file on the local file system to save the file to. If this is the
        /// name of an existing directory, the file will be downloaded to that directory.</param>
        /// <param name="progressCallback">The <see cref="ProgressCallback"/> that will be called to report progress of the operation. May be <see langword="null"/>.</param>
        public void DownloadFile(string dfsPath, string localPath, ProgressCallback progressCallback)
        {
            if( dfsPath == null )
                throw new ArgumentNullException("dfsPath");
            if( localPath == null )
                throw new ArgumentNullException("localPath");

            if( System.IO.Directory.Exists(localPath) )
            {
                int index = dfsPath.LastIndexOf(DfsPath.DirectorySeparator);
                if( index < 0 || index + 1 >= dfsPath.Length )
                {
                    throw new ArgumentException("Invalid DFS path.");
                }
                localPath = System.IO.Path.Combine(localPath, dfsPath.Substring(index + 1));
            }
            using( System.IO.FileStream stream = System.IO.File.Create(localPath) )
            {
                DownloadStream(dfsPath, stream, progressCallback);
            }
        }

        /// <summary>
        /// Downloads the files in the specified directory on the distributed file system.
        /// </summary>
        /// <param name="dfsPath">The directory on the distributed file system to download.</param>
        /// <param name="localPath">The local directory to store the files.</param>
        /// <remarks>
        /// This function is not recursive; it will only download the files that are direct children of the
        /// specified directory.
        /// </remarks>
        public void DownloadDirectory(string dfsPath, string localPath)
        {
            DownloadDirectory(dfsPath, localPath, null);
        }
        
        /// <summary>
        /// Downloads the files in the specified directory on the distributed file system.
        /// </summary>
        /// <param name="dfsPath">The directory on the distributed file system to download.</param>
        /// <param name="localPath">The local directory to store the files.</param>
        /// <remarks>
        /// This function is not recursive; it will only download the files that are direct children of the
        /// specified directory.
        /// </remarks>
        /// <param name="progressCallback">The <see cref="ProgressCallback"/> that will be called to report progress of the operation. May be <see langword="null"/>.</param>
        public void DownloadDirectory(string dfsPath, string localPath, ProgressCallback progressCallback)
        {
            if( dfsPath == null )
                throw new ArgumentNullException("dfsPath");
            if( localPath == null )
                throw new ArgumentNullException("localPath");

            DfsDirectory dir = NameServer.GetDirectoryInfo(dfsPath);
            if( dir == null )
                throw new DfsException("The specified directory does not exist.");
            foreach( FileSystemEntry entry in dir.Children )
            {
                DfsFile file = entry as DfsFile;
                if( file != null )
                {
                    string localFile = System.IO.Path.Combine(localPath, file.Name);
                    DownloadFile(file.FullPath, localFile, progressCallback);
                }
            }
        }

        /// <summary>
        /// Opens the specified file on the distributed file system for reading.
        /// </summary>
        /// <param name="path">The path of the file.</param>
        /// <returns>A <see cref="DfsInputStream"/> that can be used to read the contents of the file.</returns>
        public DfsInputStream OpenFile(string path)
        {
            return new DfsInputStream(NameServer, path);
        }

        /// <summary>
        /// Creates a new file with the specified path on the distributed file system.
        /// </summary>
        /// <param name="path">The path containing the directory and name of the file to create.</param>
        /// <returns>A <see cref="DfsOutputStream"/> that can be used to write data to the file.</returns>
        public DfsOutputStream CreateFile(string path)
        {
            return new DfsOutputStream(NameServer, path);
        }

        /// <summary>
        /// Creates a new file with the specified path on the distributed file system.
        /// </summary>
        /// <param name="path">The path containing the directory and name of the file to create.</param>
        /// <param name="blockSize">The block size of the new file, or zero to use the file system default block size.</param>
        /// <param name="replicationFactor">The number of replicas to create of the file's blocks, or zero to use the file system default replication factor.</param>
        /// <returns>
        /// A <see cref="DfsOutputStream"/> that can be used to write data to the file.
        /// </returns>
        public DfsOutputStream CreateFile(string path, int blockSize, int replicationFactor)
        {
            return CreateFile(path, blockSize, replicationFactor, true, RecordStreamOptions.None);
        }

        /// <summary>
        /// Creates a new file with the specified path on the distributed file system.
        /// </summary>
        /// <param name="path">The path containing the directory and name of the file to create.</param>
        /// <param name="blockSize">The block size of the new file, or zero to use the file system default block size.</param>
        /// <param name="replicationFactor">The number of replicas to create of the file's blocks, or zero to use the file system default replication factor.</param>
        /// <param name="recordOptions">The record options for the file.</param>
        /// <returns>
        /// A <see cref="DfsOutputStream"/> that can be used to write data to the file.
        /// </returns>
        public DfsOutputStream CreateFile(string path, int blockSize, int replicationFactor, RecordStreamOptions recordOptions)
        {
            return CreateFile(path, blockSize, replicationFactor, true, recordOptions);
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
        /// A <see cref="DfsOutputStream"/> that can be used to write data to the file.
        /// </returns>
        public DfsOutputStream CreateFile(string path, int blockSize, int replicationFactor, bool useLocalReplica, RecordStreamOptions recordOptions)
        {
            return new DfsOutputStream(NameServer, path, blockSize, replicationFactor, useLocalReplica, recordOptions);
        }

        private static T CreateNameServerClientInternal<T>(string hostName, int port)
        {
            string url = string.Format(System.Globalization.CultureInfo.InvariantCulture, _nameServerUrlFormat, hostName, port);
            return (T)Activator.GetObject(typeof(T), url);
        }

        private static void CopyStream(string fileName, System.IO.Stream inputStream, System.IO.Stream outputStream, ProgressCallback progressCallback)
        {
            byte[] buffer = new byte[4096];
            int bytesRead;
            int prevPercentage = -1;
            float length = inputStream.Length;
            if( progressCallback != null )
                progressCallback(fileName, 0, 0L);
            while( (bytesRead = inputStream.Read(buffer, 0, buffer.Length)) != 0 )
            {
                int percentage = (int)((inputStream.Position / length) * 100);
                if( percentage > prevPercentage )
                {
                    prevPercentage = percentage;
                    if( progressCallback != null )
                        progressCallback(fileName, percentage, inputStream.Position);
                }
                outputStream.Write(buffer, 0, bytesRead);
            }
            if( progressCallback != null )
                progressCallback(fileName, 100, inputStream.Length);
        }
    }
}
