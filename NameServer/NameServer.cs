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
using System.Diagnostics;

namespace NameServerApplication
{
    /// <summary>
    /// RPC server for the NameServer.
    /// </summary>
    public class NameServer : MarshalByRefObject, INameServerClientProtocol, INameServerHeartbeatProtocol
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(NameServer));
        private static List<IChannel> _channels = new List<IChannel>();

        private readonly int _replicationFactor;
        private readonly int _blockSize;
        private Random _random = new Random();

        private readonly FileSystem _fileSystem;
        private readonly Dictionary<ServerAddress, DataServerInfo> _dataServers = new Dictionary<ServerAddress, DataServerInfo>();
        private readonly Dictionary<Guid, BlockInfo> _blocks = new Dictionary<Guid, BlockInfo>();
        private readonly Dictionary<Guid, BlockInfo> _pendingBlocks = new Dictionary<Guid, BlockInfo>();
        private readonly Dictionary<Guid, BlockInfo> _underReplicatedBlocks = new Dictionary<Guid, BlockInfo>();
        private bool _safeMode = true;
        private System.Threading.ManualResetEvent _safeModeEvent = new System.Threading.ManualResetEvent(false);

        private NameServer()
            : this(true)
        {
        }

        private NameServer(bool replayLog)
            : this(DfsConfiguration.GetConfiguration(), true)
        {
        }

        private NameServer(DfsConfiguration config, bool replayLog)
        {
            if( config == null )
                throw new ArgumentNullException("config");
            Configuration = config;
            _replicationFactor = config.NameServer.ReplicationFactor;
            _blockSize = config.NameServer.BlockSize;
            _fileSystem = new FileSystem(this, replayLog);
            // TODO: Once leases are in place, we might not want to close the file when replaying (instead leave it open
            // to see if the original lease owner is still around); then we must also handle blocks that are still actually
            // pending here differently.
            _pendingBlocks.Clear();
        }

        public static NameServer Instance { get; private set; }

        public DfsConfiguration Configuration { get; private set; }

        public static void Run()
        {
            Run(DfsConfiguration.GetConfiguration());
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.Synchronized)]
        public static void Run(DfsConfiguration config)
        {
            if( config == null )
                throw new ArgumentNullException("config");

            //_rpcConfig = config;
            Instance = new NameServer(config, true);
            ConfigureRemoting(config);
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.Synchronized)]
        public static void Shutdown()
        {
            foreach( var channel in _channels )
                ChannelServices.UnregisterChannel(channel);
            _channels.Clear();
            Instance = null;
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

        public void RemoveFileBlocks(Tkl.Jumbo.Dfs.File file, Guid? pendingBlock)
        {
            if( pendingBlock != null )
            {
                lock( _pendingBlocks )
                {
                    BlockInfo info = _pendingBlocks[pendingBlock.Value];
                    _pendingBlocks.Remove(pendingBlock.Value);
                    MarkForDataServerDeletion(pendingBlock.Value, info);
                }
            }
            if( file.Blocks.Count > 0 )
            {
                lock( _blocks )
                {
                    lock( _underReplicatedBlocks )
                    {
                        foreach( var block in file.Blocks )
                        {
                            BlockInfo info = _blocks[block];
                            _blocks.Remove(block);
                            _underReplicatedBlocks.Remove(block);
                            MarkForDataServerDeletion(block, info);
                        }
                    }
                }
            }
        }

        public void CommitBlock(Guid blockID)
        {
            lock( _blocks )
            {
                lock( _pendingBlocks )
                {
                    BlockInfo info;
                    if( _pendingBlocks.TryGetValue(blockID, out info) )
                    {
                        _pendingBlocks.Remove(blockID);
                        _blocks.Add(blockID, info);
                        if( info.DataServers.Count < _replicationFactor )
                        {
                            lock( _underReplicatedBlocks )
                                _underReplicatedBlocks.Add(blockID, info);
                        }
                    }
                }
            }
        }

        #region IClientProtocol Members

        public int BlockSize
        {
            get { return _blockSize; }
        }

        public bool SafeMode
        {
            get { return _safeMode; }
        }

        public void CreateDirectory(string path)
        {
            CheckSafeMode();
            _fileSystem.CreateDirectory(path);
        }

        public Directory GetDirectoryInfo(string path)
        {
            return _fileSystem.GetDirectoryInfo(path);
        }


        public BlockAssignment CreateFile(string path)
        {
            CheckSafeMode();
            Guid guid = _fileSystem.CreateFile(path);
            return AssignBlockToDataServers(guid);
        }

        public bool Delete(string path, bool recursive)
        {
            CheckSafeMode();
            return _fileSystem.Delete(path, recursive);
        }

        public File GetFileInfo(string path)
        {
            return _fileSystem.GetFileInfo(path);
        }

        public BlockAssignment AppendBlock(string path)
        {
            CheckSafeMode();
            if( _dataServers.Count < _replicationFactor )
                throw new InvalidOperationException("Insufficient data servers.");

            Guid blockId = _fileSystem.AppendBlock(path);

            return AssignBlockToDataServers(blockId);
        }

        public void CloseFile(string path)
        {
            CheckSafeMode();
            _fileSystem.CloseFile(path);
        }

        public ServerAddress[] GetDataServersForBlock(Guid blockID)
        {
            // I allow calling this even if safemode is on, but it might return an empty list in that case.
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

        public bool WaitForSafeModeOff(int timeOut)
        {
            if( _safeMode )
                if( _safeModeEvent.WaitOne(timeOut, false) )
                    Debug.Assert(!_safeMode);

            return !_safeMode;
        }

        public DfsMetrics GetMetrics()
        {
            DfsMetrics metrics = new DfsMetrics();
            lock( _blocks )
            {
                metrics.TotalBlockCount = _blocks.Count;
            }
            lock( _pendingBlocks )
            {
                metrics.PendingBlockCount = _pendingBlocks.Count;
            }
            lock( _underReplicatedBlocks )
            {
                metrics.UnderReplicatedBlockCount = _underReplicatedBlocks.Count;
            }
            lock( _dataServers )
            {
                metrics.DataServers = (from server in _dataServers.Values
                                       select server.Address).ToArray();
            }
            metrics.TotalSize = _fileSystem.TotalSize;
            return metrics;
        }

        #endregion

        #region INameServerHeartbeatProtocol Members

        public HeartbeatResponse[] Heartbeat(ServerAddress address, HeartbeatData[] data)
        {
            //_log.Debug("Data server heartbeat received.");
            if( address == null )
                throw new ArgumentNullException("address");

            DataServerInfo dataServer;
            List<HeartbeatResponse> responseList = null;
            lock( _dataServers )
            {
                bool initialContact = data != null && data.Length > 0 && data[0] is InitialHeartbeatData;
                bool serverKnown = _dataServers.TryGetValue(address, out dataServer);
                if( initialContact || !serverKnown )
                {
                    if( serverKnown && initialContact )
                    {
                        _log.WarnFormat("Data server {0} sent initial contact data but was already known; deleting previous data.", address);
                        // TODO: Once re-replication gets implemented I have to make sure it's done in such a way that it isn't
                        // possible for a block to be re-replicated before the new block report is processed.
                        RemoveDataServer(dataServer);
                    }
                    else if( !initialContact )
                        _log.WarnFormat("A new data server has reported in at {0} but didn't send initial data.", address);
                    else
                        _log.InfoFormat("A new data server has reported in at {0}", address);
                    if( address.HostName != ServerContext.Current.ClientHostName )
                        _log.Warn("The data server reported a different hostname than is indicated in the ServerContext.");
                    dataServer = new DataServerInfo(address); // TODO: Real port number
                    _dataServers.Add(address, dataServer);
                }

                if( data != null )
                {
                    foreach( HeartbeatData item in data )
                    {
                        HeartbeatResponse response = ProcessHeartbeat(item, dataServer);
                        if( response != null )
                        {
                            if( responseList == null )
                                responseList = new List<HeartbeatResponse>();
                            responseList.Add(response);
                        }
                    }
                }

                if( !dataServer.HasReportedBlocks )
                {
                    Debug.Assert(responseList == null);
                    return new[] { new HeartbeatResponse(DataServerHeartbeatCommand.ReportBlocks) };
                }

                Guid[] blocksToDelete = dataServer.GetAndClearBlocksToDelete();
                if( blocksToDelete.Length > 0 )
                {
                    if( responseList == null )
                        responseList = new List<HeartbeatResponse>();
                    responseList.Add(new DeleteBlocksHeartbeatResponse(blocksToDelete));
                }
            }

            return responseList == null ? null : responseList.ToArray();
        }

        #endregion


        private HeartbeatResponse ProcessHeartbeat(HeartbeatData data, DataServerInfo dataServer)
        {
            BlockReportHeartbeatData blockReport = data as BlockReportHeartbeatData;
            if( blockReport != null )
            {
                return ProcessBlockReport(dataServer, blockReport);
            }

            NewBlockHeartbeatData newBlock = data as NewBlockHeartbeatData;
            if( newBlock != null )
            {
                return ProcessNewBlock(dataServer, newBlock);
            }

            if( !(data is InitialHeartbeatData) )
                _log.WarnFormat("Unknown HeartbeatData type {0}.", data.GetType().AssemblyQualifiedName);
            return null;
        }

        private HeartbeatResponse ProcessNewBlock(DataServerInfo dataServer, NewBlockHeartbeatData newBlock)
        {
            if( !dataServer.HasReportedBlocks )
                throw new Exception("A new block added to an uninitialized data server."); // TODO: Handle properly.

            _log.InfoFormat("Data server {2} reports it has received block {0} of size {1}.", newBlock.BlockID, newBlock.Size, dataServer.Address);

            BlockInfo block = null;
            bool commitBlock = false;
            lock( _pendingBlocks )
            {
                if( _pendingBlocks.TryGetValue(newBlock.BlockID, out block) )
                {
                    // TODO: Should there be some kind of check whether the data server reporting this was actually
                    // one of the assigned servers?
                    block.DataServers.Add(dataServer);
                    dataServer.Blocks.Add(newBlock.BlockID);
                    if( block.DataServers.Count >= _replicationFactor )
                    {
                        commitBlock = true;
                    }
                }
                else
                {
                    // TODO: Inform the data server to delete the block. Also check if it's not an already existing block that's
                    // being re-reported?
                    _log.WarnFormat("Block {0} is not pending.", newBlock.BlockID);
                }
                // We don't need to check in _blocks; they're not moved there until all data servers have been checked in.
            }
            if( commitBlock )
            {
                _log.InfoFormat("Pending block {0} is now fully replicated and is being committed.", newBlock.BlockID);
                _fileSystem.CommitBlock(block.File.FullPath, newBlock.BlockID, newBlock.Size);
            }

            return null;
        }

        private HeartbeatResponse ProcessBlockReport(DataServerInfo dataServer, BlockReportHeartbeatData blockReport)
        {
            if( dataServer.HasReportedBlocks )
                _log.Warn("Duplicate block report, ignoring.");
            else
            {
                List<Guid> invalidBlocks = null;
                _log.Info("Received block report.");
                dataServer.HasReportedBlocks = true;
                dataServer.Blocks = new List<Guid>();
                foreach( Guid block in blockReport.Blocks )
                {
                    BlockInfo info;
                    lock( _blocks )
                    {
                        lock( _underReplicatedBlocks )
                        {
                            // TODO: It is possible for a data server that has already received and reported a block to go down
                            // and re-report before the block leaves pending state. This makes it possible for a server to report
                            // a pending block here, which needs to be dealt with.
                            if( _underReplicatedBlocks.TryGetValue(block, out info) )
                            {
                                info.DataServers.Add(dataServer);
                                _log.DebugFormat("Dataserver {0} has block ID {1}", dataServer.Address, block);
                                if( info.DataServers.Count >= _replicationFactor )
                                {
                                    _log.InfoFormat("Block {0} has reached sufficient replication level.", block);
                                    _underReplicatedBlocks.Remove(block);
                                    // Not needed, _blocks contains all non-pending blocks, even underreplicated ones: _blocks.Add(block, info); 
                                }
                                dataServer.Blocks.Add(block);
                            }
                            else if( _blocks.TryGetValue(block, out info) )
                            {
                                info.DataServers.Add(dataServer);
                                _log.DebugFormat("Dataserver {0} has block ID {1}", dataServer.Address, block);
                                dataServer.Blocks.Add(block);
                            }
                            else
                            {
                                _log.WarnFormat("Dataserver {0} reported unknown block {1}.", dataServer.Address, block);
                                if( invalidBlocks == null )
                                    invalidBlocks = new List<Guid>();
                                invalidBlocks.Add(block);
                            }
                        }
                    }
                }
                CheckDisableSafeMode();
                if( invalidBlocks != null )
                    return new DeleteBlocksHeartbeatResponse(invalidBlocks);
            }
            return null;
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
                                where server.Address.HostName == clientHostName
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
            RemotingConfiguration.RegisterWellKnownServiceType(typeof(RpcServer), "NameServer", WellKnownObjectMode.Singleton);
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
            formatter.TypeFilterLevel = System.Runtime.Serialization.Formatters.TypeFilterLevel.Full;
            formatter.Next = new ServerChannelSinkProvider();
            TcpServerChannel channel = new TcpServerChannel(properties, formatter);
            ChannelServices.RegisterChannel(channel, false);
            _channels.Add(channel);
        }

        private void CheckSafeMode()
        {
            if( _safeMode )
                throw new SafeModeException("The name server is in safe mode.");
        }

        private void CheckDisableSafeMode()
        {
            int dataServerCount;
            lock( _dataServers )
                dataServerCount = _dataServers.Count;
            int blockCount;
            lock( _underReplicatedBlocks )
                blockCount = _underReplicatedBlocks.Count;
            // TODO: After re-replication is implemented, we can disable safemode before having full replication.
            if( _safeMode && dataServerCount >= _replicationFactor && blockCount == 0 )
            {
                _safeMode = false;
                _safeModeEvent.Set();
                _log.Info("Safe mode is disabled.");
            }
        }

        private static void MarkForDataServerDeletion(Guid block, BlockInfo info)
        {
            foreach( var server in info.DataServers )
            {
                server.AddBlockToDelete(block);
            }
        }

        private void RemoveDataServer(DataServerInfo info)
        {
            lock( _dataServers )
            {
                _dataServers.Remove(info.Address);
            }
            lock( _blocks )
            {
                lock( _underReplicatedBlocks )
                {
                    lock( _pendingBlocks )
                    {
                        foreach( var blockID in info.Blocks )
                        {
                            bool pending = false;
                            BlockInfo blockInfo;
                            if( _pendingBlocks.TryGetValue(blockID, out blockInfo) )
                                pending = true;
                            else if( !_blocks.TryGetValue(blockID, out blockInfo) )
                                Debug.Assert(false); // This shouldn't happen, it means the block handling code has bugs.
                            bool removed = blockInfo.DataServers.Remove(info);
                            Debug.Assert(removed);
                            if( !pending && blockInfo.DataServers.Count < _replicationFactor && !_underReplicatedBlocks.ContainsKey(blockID) )
                            {
                                _log.InfoFormat("Block {0} is now under-replicated.", blockID);
                                _underReplicatedBlocks.Add(blockID, blockInfo);
                            }
                        }
                    }
                }
            }
        }
    }
}
