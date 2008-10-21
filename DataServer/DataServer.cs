using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Threading;
using System.Configuration;
using System.IO;

namespace DataServer
{
    class DataServer
    {
        private const int _heartbeatInterval = 2000;
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(DataServer));
        private static readonly string _blockStorageDirectory = ConfigurationManager.AppSettings["BlockStorage"];
        private static readonly string _temporaryBlockStorageDirectory = Path.Combine(_blockStorageDirectory, "temp");

        private INameServerHeartbeatProtocol _nameServer;
        private INameServerClientProtocol _nameServerClient;
        private List<HeartbeatData> _pendingHeartbeatData = new List<HeartbeatData>();

        private List<Guid> _blocks = new List<Guid>();
        private List<Guid> _pendingBlocks = new List<Guid>();
        private BlockServer _blockServer; // listens for TCP connections.
        private BlockServer _blockServerIPv4;

        public DataServer(INameServerHeartbeatProtocol nameServer, INameServerClientProtocol nameServerClient)
        {
            if( nameServer == null )
                throw new ArgumentNullException("nameServer");

            _nameServer = nameServer;
            _nameServerClient = nameServerClient;

            LoadBlocks();
        }

        public long BlockSize { get; private set; }

        public void Run()
        {
            _log.Info("Data server main loop starting.");
            BlockSize = _nameServerClient.BlockSize;
            if( System.Net.Sockets.Socket.OSSupportsIPv6 )
            {
                _blockServer = new BlockServer(this, System.Net.IPAddress.IPv6Any);
                _blockServer.RunAsync();
                _blockServerIPv4 = new BlockServer(this, System.Net.IPAddress.Any);
                _blockServerIPv4.RunAsync();
            }
            else
            {
                _blockServer = new BlockServer(this, System.Net.IPAddress.Any);
                _blockServer.RunAsync();
            }
            while( true )
            {
                SendHeartbeat();
                Thread.Sleep(_heartbeatInterval);
            }
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
            HeartbeatResponse response = _nameServer.Heartbeat(data);
            if( response != null )
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
    }
}
