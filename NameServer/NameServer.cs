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
        private readonly FileSystem _fileSystem = new FileSystem(true);
        private readonly Dictionary<string, DataServerInfo> _dataServers = new Dictionary<string, DataServerInfo>();

        public override object InitializeLifetimeService()
        {
            // This causes the object to live forever.
            return null;
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
            throw new NotImplementedException();
            //Guid blockId = _fileSystem.AppendBlock(path);
            
            // TODO: Pick data servers
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
            if( (data.Flags & HeartbeatFlags.BlockReport) != 0 )
            {
                if( dataServer.HasReportedBlocks )
                    _log.Warn("Duplicate block report, ignoring.");
                else
                {
                    _log.Info("Received block report.");
                    dataServer.HasReportedBlocks = true;
                    dataServer.Blocks = data.Blocks;
                    // TODO: Record data servers per block.
                }
            }
        }
    }
}
