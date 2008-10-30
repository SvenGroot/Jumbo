using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Runtime.Remoting.Messaging;
using System.Configuration;
using System.Collections;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting;

namespace NameServerApplication
{
    /// <summary>
    /// RPC server for the NameServer.
    /// </summary>
    class NameServer : MarshalByRefObject, INameServerClientProtocol, INameServerHeartbeatProtocol
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(NameServer));
        private static DfsConfiguration _rpcConfig; // This is set by the Run method and used by the default constructor when Remoting creates the object
        private readonly int _replicationFactor;
        private readonly int _blockSize;
        private Random _random = new Random();

        private readonly FileSystem _fileSystem;
        private readonly Dictionary<ServerAddress, DataServerInfo> _dataServers = new Dictionary<ServerAddress, DataServerInfo>();
        private readonly Dictionary<Guid, BlockInfo> _blocks = new Dictionary<Guid, BlockInfo>();
        private readonly Dictionary<Guid, BlockInfo> _pendingBlocks = new Dictionary<Guid, BlockInfo>();
        private readonly Dictionary<Guid, BlockInfo> _underReplicatedBlocks = new Dictionary<Guid, BlockInfo>();

        public NameServer()
            : this(true)
        {
        }

        public NameServer(bool replayLog)
            : this(_rpcConfig ?? DfsConfiguration.GetConfiguration(), true)
        {
        }

        public NameServer(DfsConfiguration config, bool replayLog)
        {
            if( config == null )
                throw new ArgumentNullException("config");
            Configuration = config;
            _replicationFactor = config.NameServer.ReplicationFactor;
            _blockSize = config.NameServer.BlockSize;
            _fileSystem = new FileSystem(this, replayLog);
            // After replaying the log files any pending blocks are considered to be underreplicated because data files
            // should already have them. 
            foreach( var block in _pendingBlocks )
            {
                // If the file's block list doesn't contain this block, it means there was no commit command for that block in 
                // the log file.
                if( block.Value.File.Blocks.Contains(block.Key) )
                    _underReplicatedBlocks.Add(block.Key, block.Value);
            }
            _pendingBlocks.Clear();
        }

        public DfsConfiguration Configuration { get; private set; }

        public static void Run()
        {
            Run(DfsConfiguration.GetConfiguration());
        }

        public static void Run(DfsConfiguration config)
        {
            if( config == null )
                throw new ArgumentNullException("config");

            _rpcConfig = config;
            ConfigureRemoting(config);
        }

        public override object InitializeLifetimeService()
        {
            // This causes the object to live forever.
            return null;
        }

        public void CheckBlockReplication(IEnumerable<Guid> blocks)
        {
            // TODO: Implement
        }

        public FileSystem FileSystem
        {
            get { return _fileSystem; }
        }

        public void NotifyNewBlock(File file, Guid blockId)
        {
            // Called by FileSystem when a block is added to a file.
            lock( _pendingBlocks )
            {
                _pendingBlocks.Add(blockId, new BlockInfo(file));
            }
        }

        #region IClientProtocol Members

        public int BlockSize
        {
            get { return _blockSize; }
        }

        public void CreateDirectory(string path)
        {
            _fileSystem.CreateDirectory(path);
        }

        public Directory GetDirectoryInfo(string path)
        {
            return _fileSystem.GetDirectoryInfo(path);
        }


        public BlockAssignment CreateFile(string path)
        {
            Guid guid = _fileSystem.CreateFile(path);
            return AssignBlockToDataServers(guid);
        }

        public bool Delete(string path, bool recursive)
        {
            return _fileSystem.Delete(path, recursive);
        }

        public File GetFileInfo(string path)
        {
            return _fileSystem.GetFileInfo(path);
        }

        public BlockAssignment AppendBlock(string path)
        {
            if( _dataServers.Count < _replicationFactor )
                throw new InvalidOperationException("Insufficient data servers.");

            Guid blockId = _fileSystem.AppendBlock(path);

            return AssignBlockToDataServers(blockId);
        }

        public void CloseFile(string path)
        {
            _fileSystem.CloseFile(path);
        }

        public ServerAddress[] GetDataServersForBlock(Guid blockID)
        {
            lock( _blocks )
            {
                // TODO: Deal with under-replicated blocks.
                BlockInfo block;
                if( !_blocks.TryGetValue(blockID, out block) )
                    throw new ArgumentException("Invalid block ID.");

                return (from server in block.DataServers
                        select server.Address).ToArray();
            }
        }

        #endregion

        #region INameServerHeartbeatProtocol Members

        public HeartbeatResponse Heartbeat(ServerAddress address, HeartbeatData[] data)
        {
            //_log.Debug("Data server heartbeat received.");
            if( address == null )
                throw new ArgumentNullException("address");

            DataServerInfo dataServer;
            lock( _dataServers )
            {
                if( !_dataServers.TryGetValue(address, out dataServer) )
                {
                    _log.InfoFormat("A new data server has reported in at {0}", address);
                    if( address.HostName != ServerContext.Current.ClientHostName )
                        _log.Warn("The data server reported a different hostname than is indicated in the ServerContext.");
                    dataServer = new DataServerInfo(address); // TODO: Real port number
                    _dataServers.Add(address, dataServer);
                }

                if( data != null )
                {
                    foreach( HeartbeatData item in data )
                        ProcessHeartbeat(item, dataServer);
                }

                if( !dataServer.HasReportedBlocks )
                {
                    return new HeartbeatResponse(DataServerHeartbeatCommand.ReportBlocks);
                }
            }

            return null;
        }

        #endregion


        private void ProcessHeartbeat(HeartbeatData data, DataServerInfo dataServer)
        {
            BlockReportHeartbeatData blockReport = data as BlockReportHeartbeatData;
            if( blockReport != null )
            {
                if( dataServer.HasReportedBlocks )
                    _log.Warn("Duplicate block report, ignoring.");
                else
                {
                    _log.Info("Received block report.");
                    dataServer.HasReportedBlocks = true;
                    dataServer.Blocks = new List<Guid>(blockReport.Blocks);
                    foreach( Guid block in dataServer.Blocks )
                    {
                        BlockInfo info;
                        lock( _blocks )
                        {
                            lock( _underReplicatedBlocks )
                            {
                                if( _blocks.TryGetValue(block, out info) )
                                {
                                    info.DataServers.Add(dataServer);
                                    _log.DebugFormat("Dataserver {0} has block ID {1}", dataServer.Address, block);
                                }
                                else if( _underReplicatedBlocks.TryGetValue(block, out info) )
                                {
                                    info.DataServers.Add(dataServer);
                                    _log.DebugFormat("Dataserver {0} has block ID {1}", dataServer.Address, block);
                                    if( info.DataServers.Count >= _replicationFactor )
                                    {
                                        _log.InfoFormat("Block {0} has reached sufficient replication level.", block);
                                        _underReplicatedBlocks.Remove(block);
                                        _blocks.Add(block, info);
                                    }
                                }
                                else
                                {
                                    _log.WarnFormat("Dataserver {0} reported unknown block {1}.", dataServer.Address, block);
                                    // TODO: Inform the data server to delete the block.
                                }
                            }
                        }
                    }
                }
            }

            NewBlockHeartbeatData newBlock = data as NewBlockHeartbeatData;
            if( newBlock != null )
            {
                if( !dataServer.HasReportedBlocks )
                    throw new Exception("A new block added to an uninitialized data server."); // TODO: Handle properly.

                _log.InfoFormat("Data server {2} reports it has received block {0} of size {1}.", newBlock.BlockID, newBlock.Size, dataServer.Address);

                BlockInfo block;
                lock( _blocks )
                lock( _pendingBlocks )
                {
                    if( _pendingBlocks.TryGetValue(newBlock.BlockID, out block) )
                    {
                        // TODO: Should there be some kind of check whether the data server reporting this was actually
                        // one of the assigned servers?
                        block.DataServers.Add(dataServer);
                        if( block.DataServers.Count >= _replicationFactor )
                        {
                            // TODO: We need to record the total size of the file somewhere, and record that in the file system only when
                            // the file is completed.
                            _log.InfoFormat("Pending block {0} is now fully replicated and is being committed.", newBlock.BlockID);
                            _fileSystem.CommitBlock(block.File.FullPath, newBlock.BlockID, newBlock.Size);
                            _pendingBlocks.Remove(newBlock.BlockID);
                            _blocks.Add(newBlock.BlockID, block);
                        }
                    }
                    else
                    {
                        // TODO: Inform the data server to delete the block.
                        _log.WarnFormat("Block {0} is not pending.", newBlock.BlockID);
                    }
                    // We don't need to check in _blocks; they're not moved there until all data servers have been checked in.
                }
            }
        }

        private BlockAssignment AssignBlockToDataServers(Guid blockId)
        {
            // TODO: This really ought to be checked before the CreateFile is logged.
            if( _dataServers.Count < _replicationFactor )
                throw new DfsException("Insufficient data servers to replicate new block.");

            // TODO: Better selection policy.
            List<DataServerInfo> unassignedDataServers;
            lock( _dataServers )
            {
                unassignedDataServers = new List<DataServerInfo>(_dataServers.Values);
            }

            int serversNeeded = _replicationFactor;
            List<ServerAddress> dataServers = new List<ServerAddress>(_replicationFactor);

            // Check if any data servers are running on the client's own system.
            var clientHostName = ServerContext.Current.ClientHostName;
            var localServers = (from server in unassignedDataServers
                                where server.Address.HostName == ServerContext.Current.ClientHostName
                                select server).ToArray();

            if( localServers.Length > 0 )
            {
                if( localServers.Length == 1 )
                {
                    dataServers.Add(localServers[0].Address);
                    unassignedDataServers.Remove(localServers[0]);
                }
                else
                {
                    int server = _random.Next(localServers.Length);
                    dataServers.Add(localServers[server].Address);
                    unassignedDataServers.Remove(localServers[server]);
                }
                --serversNeeded;
            }

            for( int x = 0; x < serversNeeded; ++x )
            {
                int server = _random.Next(unassignedDataServers.Count);
                dataServers.Add(unassignedDataServers[server].Address);
                unassignedDataServers.RemoveAt(server);
            }

            if( _log.IsInfoEnabled )
            {
                foreach( ServerAddress address in dataServers )
                    _log.InfoFormat("Assigned data server for block {0}: {1}", blockId, address);
            }

            return new BlockAssignment() { BlockID = blockId, DataServers = dataServers };
        }

        private static void ConfigureRemoting(DfsConfiguration config)
        {
            if( System.Net.Sockets.Socket.OSSupportsIPv6 )
            {
                RegisterChannel(config, "[::]", "tcp6");
                if( config.NameServer.ListenIPv4AndIPv6 )
                    RegisterChannel(config, "0.0.0.0", "tcp4");
            }
            else
                RegisterChannel(config, null, null);
            RemotingConfiguration.RegisterWellKnownServiceType(typeof(NameServer), "NameServer", WellKnownObjectMode.Singleton);
            _log.Info("RPC server started.");
        }

        private static void RegisterChannel(DfsConfiguration config, string bindTo, string name)
        {
            IDictionary properties = new Hashtable();
            if( name != null )
                properties["name"] = name;
            properties["port"] = config.NameServer.Port;
            if( bindTo != null )
                properties["bindTo"] = bindTo;
            BinaryServerFormatterSinkProvider formatter = new BinaryServerFormatterSinkProvider();
            formatter.Next = new ServerChannelSinkProvider();
            TcpChannel channel = new TcpChannel(properties, null, formatter);
            ChannelServices.RegisterChannel(channel, false);
        }
    }
}
