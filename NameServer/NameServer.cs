﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Runtime.Remoting.Messaging;
using System.Configuration;

namespace NameServer
{
    /// <summary>
    /// RPC server for the NameServer.
    /// </summary>
    class NameServer : MarshalByRefObject, INameServerClientProtocol, INameServerHeartbeatProtocol
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(NameServer));
        private const int _replicationFactor = 1; // TODO: Replace with configuration value
        private static readonly int _blockSize = Convert.ToInt32(ConfigurationManager.AppSettings["BlockSize"]);
        private Random _random = new Random();

        private readonly FileSystem _fileSystem;
        private readonly Dictionary<string, DataServerInfo> _dataServers = new Dictionary<string, DataServerInfo>();
        private readonly Dictionary<Guid, BlockInfo> _blocks = new Dictionary<Guid, BlockInfo>();
        private readonly Dictionary<Guid, BlockInfo> _pendingBlocks = new Dictionary<Guid, BlockInfo>();
        private readonly Dictionary<Guid, BlockInfo> _underReplicatedBlocks = new Dictionary<Guid, BlockInfo>();

        public NameServer()
            : this(true)
        {
        }

        public NameServer(bool replayLog)
        {
            _fileSystem = new FileSystem(this, replayLog);
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

        #endregion

        #region INameServerHeartbeatProtocol Members

        public HeartbeatResponse Heartbeat(HeartbeatData[] data)
        {
            //_log.Debug("Data server heartbeat received.");

            string hostName = ServerContext.Current.ClientHostName;
            DataServerInfo dataServer;
            lock( _dataServers )
            {
                if( !_dataServers.TryGetValue(hostName, out dataServer) )
                {
                    _log.Info("A new data server has reported in.");
                    dataServer = new DataServerInfo(hostName);
                    _dataServers.Add(hostName, dataServer);
                }

                foreach( HeartbeatData item in data )
                    ProcessHeartbeat(item, dataServer);

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
                    // TODO: Record data servers per block.
                    foreach( Guid block in dataServer.Blocks )
                    {
                        BlockInfo info;
                        lock( _blocks )
                        {
                            if( _blocks.TryGetValue(block, out info) )
                            {
                                info.DataServers.Add(dataServer);
                            }
                        }
                        // TODO: Underreplicated blocks (pending blocks not possible here).
                    }
                }
            }

            NewBlockHeartbeatData newBlock = data as NewBlockHeartbeatData;
            if( newBlock != null )
            {
                if( !dataServer.HasReportedBlocks )
                    throw new Exception("A new block added to an uninitialized data server."); // TODO: Handle properly.

                _log.InfoFormat("Data server reports it has received block {0} of size {1}.", newBlock.BlockID, newBlock.Size);

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
                            // TODO: We need to update the size of the file, and log that as well in the edit log.
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
            // TODO: Better selection policy.
            List<DataServerInfo> unassignedDataServers = new List<DataServerInfo>(_dataServers.Values);
            List<string> dataServers = new List<string>(_replicationFactor);
            for( int x = 0; x < _replicationFactor; ++x )
            {
                int server = _random.Next(unassignedDataServers.Count);
                dataServers.Add(unassignedDataServers[x].HostName);
                unassignedDataServers.RemoveAt(x);
            }
            return new BlockAssignment() { BlockID = blockId, DataServers = dataServers };
        }
    }
}
