using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Threading;
using System.Configuration;
using System.IO;

namespace DataServerApplication
{
    public class DataServer
    {
        private const int _heartbeatInterval = 2000;
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(DataServer));
        private readonly string _blockStorageDirectory;
        private readonly string _temporaryBlockStorageDirectory;
        private readonly int _port;
        private readonly DfsConfiguration _config;

        private INameServerHeartbeatProtocol _nameServer;
        private INameServerClientProtocol _nameServerClient;
        private List<HeartbeatData> _pendingHeartbeatData = new List<HeartbeatData>();

        private List<Guid> _blocks = new List<Guid>();
        private List<Guid> _pendingBlocks = new List<Guid>();
        private BlockServer _blockServer; // listens for TCP connections.
        private BlockServer _blockServerIPv4;
        private volatile bool _running;

        public DataServer()
            : this(DfsConfiguration.GetConfiguration())
        {
        }

        public DataServer(DfsConfiguration config)
        {
            if( config == null )
                throw new ArgumentNullException("config");

            _config = config;
            _blockStorageDirectory = config.DataServer.BlockStoragePath;
            _temporaryBlockStorageDirectory = Path.Combine(_blockStorageDirectory, "temp");
            System.IO.Directory.CreateDirectory(_temporaryBlockStorageDirectory);
            _port = config.DataServer.Port;
            _nameServer = DfsClient.CreateNameServerHeartbeatClient(config);
            _nameServerClient = DfsClient.CreateNameServerClient(config);

            LoadBlocks();
        }

        public long BlockSize { get; private set; }

        public ServerAddress LocalAddress { get; private set; }

        public void Run()
        {
            _running = true;
            LocalAddress = new ServerAddress(System.Net.Dns.GetHostName(), _port);

            _log.Info("Data server main loop starting.");
            BlockSize = _nameServerClient.BlockSize;
            if( System.Net.Sockets.Socket.OSSupportsIPv6 )
            {
                _blockServer = new BlockServer(this, System.Net.IPAddress.IPv6Any, _port);
                _blockServer.RunAsync();
                if( _config.DataServer.ListenIPv4AndIPv6 )
                {
                    _blockServerIPv4 = new BlockServer(this, System.Net.IPAddress.Any, _port);
                    _blockServerIPv4.RunAsync();
                }
            }
            else
            {
                _blockServer = new BlockServer(this, System.Net.IPAddress.Any, _port);
                _blockServer.RunAsync();
            }

            AddDataForNextHeartbeat(new InitialHeartbeatData());

            while( _running )
            {
                SendHeartbeat();
                Thread.Sleep(_heartbeatInterval);
            }
        }

        public void Abort()
        {
            if( _blockServer != null )
            {
                _blockServer.Abort();
                _blockServer = null;
            }
            if( _blockServerIPv4 != null )
            {
                _blockServerIPv4.Abort();
                _blockServerIPv4 = null;
            }
            _running = false;
        }

        public FileStream AddNewBlock(Guid blockID)
        {
            lock( _blocks )
            lock( _pendingBlocks )
            {
                if( _blocks.Contains(blockID) || _pendingBlocks.Contains(blockID) )
                    throw new ArgumentException("Existing block ID.");
                _pendingBlocks.Add(blockID);
                System.IO.Directory.CreateDirectory(_temporaryBlockStorageDirectory);
                return System.IO.File.Create(Path.Combine(_temporaryBlockStorageDirectory, blockID.ToString()));
            }
        }

        public FileStream OpenBlock(Guid blockID)
        {
            lock( _blocks )
            {
                if( !_blocks.Contains(blockID) )
                    throw new ArgumentException("Invalid block.");

                return System.IO.File.OpenRead(Path.Combine(_blockStorageDirectory, blockID.ToString()));
            }
        }

        public int GetBlockSize(Guid blockID)
        {
            lock( _blocks )
            {
                if( !_blocks.Contains(blockID) )
                    throw new ArgumentException("Invalid block.");

                return (int)new FileInfo(Path.Combine(_blockStorageDirectory, blockID.ToString())).Length;
            }
        }

        public void CompleteBlock(Guid blockID, int size)
        {
            lock( _blocks )
            lock( _pendingBlocks )
            {
                if( !_pendingBlocks.Contains(blockID) || _blocks.Contains(blockID) )
                    throw new ArgumentException("Invalid block ID.");

                _pendingBlocks.Remove(blockID);
                System.IO.File.Move(Path.Combine(_temporaryBlockStorageDirectory, blockID.ToString()), Path.Combine(_blockStorageDirectory, blockID.ToString()));
                _blocks.Add(blockID);
            }
            AddDataForNextHeartbeat(new NewBlockHeartbeatData() { BlockID = blockID, Size = size });
            // We send the heartbeat immediately so the client knows that when the server comes back to him, the name server
            // knows about the block being committed.
            SendHeartbeat();
        }

        private void SendHeartbeat()
        {
            //_log.Debug("Sending heartbeat to name server.");
            HeartbeatData[] data = null;
            lock( _pendingHeartbeatData )
            {
                // TODO: Maybe we should not clear the list until we know sending the heartbeatdata has succeeded?
                if( _pendingHeartbeatData.Count > 0 )
                {
                    data = _pendingHeartbeatData.ToArray();
                    _pendingHeartbeatData.Clear();
                }
            }
            HeartbeatResponse[] response = _nameServer.Heartbeat(LocalAddress, data);
            if( response != null )
                ProcessResponses(response);
        }

        private void ProcessResponses(HeartbeatResponse[] responses)
        {
            foreach( var response in responses )
                ProcessResponse(response);
        }

        private void ProcessResponse(HeartbeatResponse response)
        {
            switch( response.Command )
            {
            case DataServerHeartbeatCommand.ReportBlocks:
                _log.Info("Received ReportBlocks command.");
                HeartbeatData data;
                lock( _blocks )
                {
                    data = new BlockReportHeartbeatData() { Blocks = _blocks.ToArray() };
                }
                AddDataForNextHeartbeat(data);
                break;
            case DataServerHeartbeatCommand.DeleteBlocks:
                _log.Info("Received DeleteBlocks command.");
                DeleteBlocks(((DeleteBlocksHeartbeatResponse)response).Blocks);
                break;
            }
        }

        private void AddDataForNextHeartbeat(HeartbeatData data)
        {
            lock( _pendingHeartbeatData )
            {
                _pendingHeartbeatData.Add(data);
            }
        }

        private void LoadBlocks()
        {
            // Since this'll be likely only done on object construction, the lock isn't strictly needed.
            // It doesn't hurt though.
            lock( _blocks )
            {
                _log.InfoFormat("Loading blocks...");
                string[] files = System.IO.Directory.GetFiles(_blockStorageDirectory);
                foreach( string file in files )
                {
                    string fileName = Path.GetFileName(file);
                    try
                    {
                        Guid blockID = new Guid(fileName);
                        _log.DebugFormat("- Block ID: {0}", blockID);
                        _blocks.Add(blockID);
                    }
                    catch( FormatException )
                    {
                        _log.WarnFormat("The name of file '{0}' in the block storage directory is not a valid GUID.", fileName);
                    }
                }
            }
        }

        private void DeleteBlocks(IEnumerable<Guid> blocks)
        {
            lock( _blocks )
            {
                foreach( var block in blocks )
                {
                    _log.InfoFormat("Removing block {0}.", block);
                    _blocks.Remove(block);
                }
            }
            foreach( var block in blocks )
            {
                try
                {
                    System.IO.File.Delete(Path.Combine(_blockStorageDirectory, block.ToString()));
                }
                catch( IOException ex )
                {
                    _log.Error(string.Format("Failed to delete block {0}.", block), ex);
                }
            }
        }
    }
}
