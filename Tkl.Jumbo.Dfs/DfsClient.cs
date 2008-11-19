using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;

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
        public DfsClient(DfsConfiguration config)
        {
            NameServer = CreateNameServerClient(config);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsClient"/> class using the specified host name and port.
        /// </summary>
        /// <param name="hostName">The host name of the name server.</param>
        /// <param name="port">The port at which the name server is listening.</param>
        public DfsClient(string hostName, int port)
        {
            NameServer = CreateNameServerClient(hostName, port);
        }

        /// <summary>
        /// Gets or sets the <see cref="INameServerClientProtocol"/> used by this instance to communicate with the name server.
        /// </summary>
        public INameServerClientProtocol NameServer { get; private set; }

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
        /// Uploads the contents of the specified stream to the Distributed File System.
        /// </summary>
        /// <param name="stream">The stream with the data to upload.</param>
        /// <param name="dfsPath">The path of the file on the DFS to write the data to.</param>
        public void UploadStream(System.IO.Stream stream, string dfsPath)
        {
            if( dfsPath == null )
                throw new ArgumentNullException("dfsPath");
            if( stream == null )
                throw new ArgumentNullException("stream");

            using( DfsOutputStream outputStream = new DfsOutputStream(NameServer, dfsPath) )
            {
                CopyStream(stream, outputStream);
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
            if( dfsPath == null )
                throw new ArgumentNullException("dfsPath");
            if( localPath == null )
                throw new ArgumentNullException("localPath");
            Directory dir = NameServer.GetDirectoryInfo(dfsPath);
            if( dir != null )
            {
                string fileName = System.IO.Path.GetFileName(localPath);
                if( !dfsPath.EndsWith(DfsPath.DirectorySeparator.ToString()) )
                    dfsPath += DfsPath.DirectorySeparator;
                dfsPath += fileName;
            }
            using( System.IO.FileStream inputStream = System.IO.File.OpenRead(localPath) )
            {
                UploadStream(inputStream, dfsPath);
            }
        }

        /// <summary>
        /// Downloads the specified file from the DFS, saving it to the specified stream.
        /// </summary>
        /// <param name="dfsPath">The path of the file on the DFS to download.</param>
        /// <param name="stream">The stream to save the file to.</param>
        public void DownloadStream(string dfsPath, System.IO.Stream stream)
        {
            if( dfsPath == null )
                throw new ArgumentNullException("dfsPath");
            if( stream == null )
                throw new ArgumentNullException("stream");
            using( DfsInputStream inputStream = new DfsInputStream(NameServer, dfsPath) )
            {
                CopyStream(inputStream, stream);
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
                DownloadStream(dfsPath, stream);
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
            if( dfsPath == null )
                throw new ArgumentNullException("dfsPath");
            if( localPath == null )
                throw new ArgumentNullException("localPath");

            Directory dir = NameServer.GetDirectoryInfo(dfsPath);
            if( dir == null )
                throw new DfsException("The specified directory does not exist.");
            foreach( FileSystemEntry entry in dir.Children )
            {
                File file = entry as File;
                if( file != null )
                {
                    string localFile = System.IO.Path.Combine(localPath, file.Name);
                    DownloadFile(file.FullPath, localFile);
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

        private static T CreateNameServerClientInternal<T>(string hostName, int port)
        {
            string url = string.Format(System.Globalization.CultureInfo.InvariantCulture, _nameServerUrlFormat, hostName, port);
            return (T)Activator.GetObject(typeof(T), url);
        }

        private static void CopyStream(System.IO.Stream inputStream, System.IO.Stream outputStream)
        {
            byte[] buffer = new byte[_bufferSize];
            int bytesRead;
            int prevPercentage = -1;
            while( (bytesRead = inputStream.Read(buffer, 0, buffer.Length)) != 0 )
            {
                int percentage = (int)((inputStream.Position / (float)inputStream.Length) * 100);
                if( percentage > prevPercentage )
                {
                    prevPercentage = percentage;
                }
                outputStream.Write(buffer, 0, bytesRead);
            }
        }
    }
}
