using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Runtime.Remoting.Messaging;

namespace NameServer
{
    /// <summary>
    /// RPC server for the NameServer.
    /// </summary>
    class NameServer : MarshalByRefObject, INameServerClientProtocol, INameServerHeartbeatProtocol
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(NameServer));
        private const int _replicationFactor = 1; // TODO: Replace with configuration value
        private Random _random = new Random();

        private readonly FileSystem _fileSystem;
        private readonly Dictionary<string, DataServerInfo> _dataServers = new Dictionary<string, DataServerInfo>();
        private readonly Dictionary<Guid, BlockInfo> _blocks = new Dictionary<Guid, BlockInfo>();
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
            lock( _underReplicatedBlocks )
            {
                _underReplicatedBlocks.Add(blockId, new BlockInfo(file));
            }
        }

        #region IClientProtocol Members

        public void CreateDirectory(string path)
        {
            _fileSystem.CreateDirectory(path);
        }

        public Directory GetDirectoryInfo(string path)
        {
            return _fileSystem.GetDirectoryInfo(path);
        }


        public void CreateFile(string path)
        {
            _fileSystem.CreateFile(path);
        }

        public File GetFileInfo(string path)
        {
            return _fileSystem.GetFileInfo(path);
        }

        public Block AppendBlock(string path)
        {
            if( _dataServers.Count < _replicationFactor )
                throw new InvalidOperationException("Insufficient data servers.");

            Guid blockId = _fileSystem.AppendBlock(path);
            
            // TODO: Better selection policy.
            List<DataServerInfo> unassignedDataServers = new List<DataServerInfo>(_dataServers.Values);
            List<string> dataServers = new List<string>(_replicationFactor);
            for( int x = 0; x < _replicationFactor; ++x )
            {
                int server = _random.Next(unassignedDataServers.Count);
                dataServers.Add(unassignedDataServers[x].HostName);
                unassignedDataServers.RemoveAt(x);
            }
            return new Block() { BlockID = blockId, DataServers = dataServers };
        }

        #endregion

        #region INameServerHeartbeatProtocol Members

        public HeartbeatResponse Heartbeat(HeartbeatData data)
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

                ProcessHeartbeat(data, dataServer);

                if( !dataServer.HasReportedBlocks )
                {
                    return new HeartbeatResponse(DataServerCommand.ReportBlocks);
                }
            }

            return null;
        }

        #endregion


        private void ProcessHeartbeat(HeartbeatData data, DataServerInfo dataServer)
        {
            BlockReportData blockReport = data as BlockReportData;
            if( blockReport != null )
            {
                if( dataServer.HasReportedBlocks )
                    _log.Warn("Duplicate block report, ignoring.");
                else
                {
                    _log.Info("Received block report.");
                    dataServer.HasReportedBlocks = true;
                    dataServer.Blocks = blockReport.Blocks;
                    // TODO: Record data servers per block.
                }
            }
        }
    }
}
