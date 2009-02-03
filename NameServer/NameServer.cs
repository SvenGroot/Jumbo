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
using Tkl.Jumbo;

namespace NameServerApplication
{
    /// <summary>
    /// RPC server for the NameServer.
    /// </summary>
    public class NameServer : MarshalByRefObject, INameServerClientProtocol, INameServerHeartbeatProtocol
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(NameServer));

        private readonly int _replicationFactor;
        private readonly int _blockSize;
        private Random _random = new Random();

        private readonly FileSystem _fileSystem;
        private readonly Dictionary<ServerAddress, DataServerInfo> _dataServers = new Dictionary<ServerAddress, DataServerInfo>();
        private readonly Dictionary<Guid, BlockInfo> _blocks = new Dictionary<Guid, BlockInfo>();
        private readonly Dictionary<Guid, PendingBlock> _pendingBlocks = new Dictionary<Guid, PendingBlock>();
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
            // TODO: Once leases are in place, we probably shouldn't clear the _pendingBlocks collection.
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

            _log.Info("---- NameServer is starting ----");
            _log.LogEnvironmentInformation();
            
            Instance = new NameServer(config, true);
            ConfigureRemoting(config);
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.Synchronized)]
        public static void Shutdown()
        {
            RpcHelper.UnregisterServerChannels(Instance.Configuration.NameServer.Port);
            Instance = null;
            _log.Info("---- NameServer has shut down ----");
        }

        public override object InitializeLifetimeService()
        {
            // This causes the object to live forever.
            return null;
        }

        public void CheckBlockReplication(IEnumerable<Guid> blocks)
        {
            // I believe this is what Hadoop does, but is it the right thing to do?
            lock( _underReplicatedBlocks )
            {
                foreach( Guid blockID in blocks )
                {
                    BlockInfo info;
                    if( _underReplicatedBlocks.TryGetValue(blockID, out info) )
                        throw new DfsException("Cannot add a block to a file with under-replicated blocks.");
                }
            }
        }

        public FileSystem FileSystem
        {
            get { return _fileSystem; }
        }

        public void NotifyNewBlock(File file, Guid blockId)
        {
            // Called by FileSystem when a pending block is added to a file (AppendBlock)
            lock( _pendingBlocks )
            {
                _pendingBlocks.Add(blockId, new PendingBlock(new BlockInfo(file)));
            }
        }

        public void RemoveFileBlocks(Tkl.Jumbo.Dfs.File file, Guid? pendingBlock)
        {
            if( pendingBlock != null )
            {
                lock( _pendingBlocks )
                {
                    PendingBlock info = _pendingBlocks[pendingBlock.Value];
                    _pendingBlocks.Remove(pendingBlock.Value);
                    MarkForDataServerDeletion(pendingBlock.Value, info.Block);
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

        public void DiscardBlock(Guid blockID)
        {
            lock( _pendingBlocks )
            {
                _pendingBlocks.Remove(blockID);
            }
        }

        public void CommitBlock(Guid blockID)
        {
            lock( _blocks )
            {
                lock( _pendingBlocks )
                {
                    PendingBlock pendingBlock;
                    if( _pendingBlocks.TryGetValue(blockID, out pendingBlock) )
                    {
                        _pendingBlocks.Remove(blockID);
                        _blocks.Add(blockID, pendingBlock.Block);
                        // This can happen during log file replay or if a server crashed between commits.
                        if( pendingBlock.Block.DataServers.Count < _replicationFactor )
                        {
                            lock( _underReplicatedBlocks )
                                _underReplicatedBlocks.Add(blockID, pendingBlock.Block);
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
            try
            {
                return AssignBlockToDataServers(guid);
            }
            catch( Exception )
            {
                CloseFile(path);
                Delete(path, false);
                throw;
            }
        }

        public bool Delete(string path, bool recursive)
        {
            CheckSafeMode();
            return _fileSystem.Delete(path, recursive);
        }

        public void Move(string from, string to)
        {
            CheckSafeMode();
            _fileSystem.Move(from, to);
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
                BlockInfo block;
                if( !_blocks.TryGetValue(blockID, out block) )
                    throw new ArgumentException("Invalid block ID.");

                var localServers = from server in block.DataServers
                                   where server.Address.HostName == ServerContext.Current.ClientHostName 
                                   select server.Address;
                var remoteServers = from server in block.DataServers
                                    where server.Address.HostName != ServerContext.Current.ClientHostName 
                                    select server.Address;
                return localServers.Concat(remoteServers).ToArray();
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
                                       select new DataServerMetrics()
                                       {
                                           Address = server.Address,
                                           LastContactUtc = server.LastContactUtc,
                                           BlockCount = server.Blocks.Count,
                                           DiskSpaceFree = server.DiskSpaceFree,
                                           DiskSpaceUsed = server.DiskSpaceUsed
                                       }).ToArray();
            }
            metrics.TotalSize = _fileSystem.TotalSize;
            return metrics;
        }

        public int GetDataServerBlockCount(ServerAddress dataServer, Guid[] blocks)
        {
            _log.DebugFormat("GetDataServerBlockCount, dataServer = {{{0}}}", dataServer);
            if( dataServer == null )
                throw new ArgumentNullException("dataServer");
            if( blocks == null )
                throw new ArgumentNullException("blocks");
            lock( _dataServers )
            {
                DataServerInfo server;
                if( !_dataServers.TryGetValue(dataServer, out server) )
                {
                    server = (from s in _dataServers.Values
                              where s.Address.HostName == dataServer.HostName
                              select s).First();
                }
                return server.Blocks.Intersect(blocks).Count();
            }
        }

        public string GetLogFileContents(int maxSize)
        {
            if( maxSize <= 0 )
                throw new ArgumentException("maxSize must be positive.", "maxSize");
            _log.Debug("GetLogFileContents");
            foreach( log4net.Appender.IAppender appender in log4net.LogManager.GetRepository().GetAppenders() )
            {
                log4net.Appender.FileAppender fileAppender = appender as log4net.Appender.FileAppender;
                if( fileAppender != null )
                {
                    using( System.IO.FileStream stream = System.IO.File.Open(fileAppender.File, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite) )
                    {
                        using( System.IO.StreamReader reader = new System.IO.StreamReader(stream) )
                        {
                            if( stream.Length > maxSize )
                            {
                                stream.Position = stream.Length - maxSize;
                                reader.ReadLine(); // Scan to the first new line.
                            }
                            return reader.ReadToEnd();
                        }
                    }
                }
            }
            return null;
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
                    dataServer = new DataServerInfo(address);
                    _dataServers.Add(address, dataServer);
                }

                dataServer.LastContactUtc = DateTime.UtcNow;

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
            StatusHeartbeatData status = data as StatusHeartbeatData;
            if( status != null )
            {
                _log.InfoFormat("Data server {0} status: {1}B used, {2}B free.", dataServer.Address, status.DiskSpaceUsed, status.DiskSpaceFree);
                dataServer.DiskSpaceFree = status.DiskSpaceFree;
                dataServer.DiskSpaceUsed = status.DiskSpaceUsed;
                // Don't return; some of the other heartbeat types inherit from StatusHeartbeatData
            }

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

            return null;
        }

        private HeartbeatResponse ProcessNewBlock(DataServerInfo dataServer, NewBlockHeartbeatData newBlock)
        {
            _log.InfoFormat("Data server {2} reports it has received block {0} of size {1}.", newBlock.BlockID, newBlock.Size, dataServer.Address);

            if( dataServer.Blocks.Contains(newBlock.BlockID) )
            {
                _log.WarnFormat("Data server {0} already had block {1}.", dataServer.Address, newBlock.BlockID);
                return null;
            }

            bool commitBlock = false;
            HeartbeatResponse response = null;
            PendingBlock pendingBlock;
            lock( _pendingBlocks )
            {
                if( _pendingBlocks.TryGetValue(newBlock.BlockID, out pendingBlock) )
                {
                    // TODO: Should there be some kind of check whether the data server reporting this was actually
                    // one of the assigned servers?
                    pendingBlock.Block.DataServers.Add(dataServer);
                    dataServer.Blocks.Add(newBlock.BlockID);
                    if( pendingBlock.IncrementCommit() >= _replicationFactor )
                    {
                        commitBlock = true;
                    }
                }
                else
                {
                    _log.WarnFormat("Block {0} is not pending.", newBlock.BlockID);
                    response = new DeleteBlocksHeartbeatResponse(new Guid[] { newBlock.BlockID });
                }
                // We don't need to check in _blocks; they're not moved there until all data servers have been checked in.
            }
            if( commitBlock )
            {
                // CommitBlock will also call back into NameServer to commit the block on this side.
                _log.InfoFormat("Pending block {0} is now fully replicated and is being committed.", newBlock.BlockID);
                _fileSystem.CommitBlock(pendingBlock.Block.File.FullPath, newBlock.BlockID, newBlock.Size);
            }

            return response;
        }

        private HeartbeatResponse ProcessBlockReport(DataServerInfo dataServer, BlockReportHeartbeatData blockReport)
        {
            /* The normal order of events is:
             * - Server checks in
             * - Server sends block report
             * - Server sends newblock heartbeat after it gets blocks
             * What can also happen (assume server already checked in)
             * - Server sends newblock
             * - Server restarts and sends block report before the block was committed.
             *   Because the commit count is different from the data server list, the block can get committed in the mean time.
             *   No action is necessary.
             * Also
             * - Server is receiving block
             * - Name server crashes and restarts
             * - Server sends newblock as the first heartbeat after the name server comes back up
             * - Server sends block report
             *   - Block could still be pending: this server is already registered: do nothing
             *   - Block no longer pending: do nothing.
             */
            if( dataServer.HasReportedBlocks )
                _log.Warn("Duplicate block report, ignoring.");
            else
            {
                List<Guid> invalidBlocks = null;
                _log.Info("Received block report.");
                dataServer.HasReportedBlocks = true;
                foreach( Guid block in blockReport.Blocks )
                {
                    BlockInfo info;
                    lock( _blocks )
                    {
                        if( _blocks.TryGetValue(block, out info) )
                        {
                            // See the explanation above for situations in which the DataServer is already present.
                            if( !info.DataServers.Contains(dataServer) )
                            {
                                _log.DebugFormat("Dataserver {0} has block ID {1}", dataServer.Address, block);
                                info.DataServers.Add(dataServer);
                                dataServer.Blocks.Add(block);
                                if( info.DataServers.Count >= _replicationFactor )
                                {
                                    lock( _underReplicatedBlocks )
                                    {
                                        if( _underReplicatedBlocks.ContainsKey(block) )
                                        {
                                            _log.InfoFormat("Block {0} has reached sufficient replication level.", block);
                                            _underReplicatedBlocks.Remove(block);
                                        }
                                    }
                                }
                            }
                            else
                                _log.WarnFormat("Dataserver {0} re-reported block ID {1}", dataServer.Address, block);
                        }
                    }
                    if( info == null )
                    {
                        PendingBlock pendingBlock;
                        lock( _pendingBlocks )
                        {
                            if( _pendingBlocks.TryGetValue(block, out pendingBlock) )
                            {
                                Debug.Assert(pendingBlock.Block.DataServers.Contains(dataServer));
                                _log.WarnFormat("Dataserver {0} re-reported block ID {1}", dataServer.Address, block);
                            }
                        }
                        if( info == null )
                        {
                            _log.WarnFormat("Dataserver {0} reported unknown block {1}.", dataServer.Address, block);
                            if( invalidBlocks == null )
                                invalidBlocks = new List<Guid>();
                            invalidBlocks.Add(block);
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
            // TODO: Better selection policy.
            List<DataServerInfo> unassignedDataServers;
            lock( _dataServers )
            {
                unassignedDataServers = (from server in _dataServers.Values
                                         where server.HasReportedBlocks
                                         select server).ToList();
            }

            if( unassignedDataServers.Count < _replicationFactor )
                throw new DfsException("Insufficient data servers to replicate new block.");
            
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
            RpcHelper.RegisterServerChannels(config.NameServer.Port, config.NameServer.ListenIPv4AndIPv6);
            RpcHelper.RegisterService(typeof(RpcServer), "NameServer");
            _log.Info("RPC server started.");
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
                            PendingBlock pendingBlock;
                            if( _pendingBlocks.TryGetValue(blockID, out pendingBlock) )
                            {
                                blockInfo = pendingBlock.Block;
                                pending = true;
                            }
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
