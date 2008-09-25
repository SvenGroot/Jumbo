using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Threading;

namespace DataServer
{
    class DataServer
    {
        private const int _heartbeatInterval = 2000;
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(DataServer));

        private INameServerHeartbeatProtocol _nameServer;
        private bool _reportBlocks = false;

        private BlockServer _blockServer = new BlockServer(); // listens for TCP connections.

        public DataServer(INameServerHeartbeatProtocol nameServer)
        {
            if( nameServer == null )
                throw new ArgumentNullException("nameServer");

            _nameServer = nameServer;
        }

        public void Run()
        {
            _log.Info("Data server main loop starting.");
            _blockServer.RunAsync();
            while( true )
            {
                SendHeartbeat();
                Thread.Sleep(_heartbeatInterval);
            }
        }

        private void SendHeartbeat()
        {
            //_log.Debug("Sending heartbeat to name server.");
            HeartbeatData data = null;
            if( _reportBlocks )
            {
                _log.Info("Sending block report.");
                _reportBlocks = false;
                data = new BlockReportData() { Blocks = new List<Guid>() };  // TODO: Real block report.
            }
            HeartbeatResponse response = _nameServer.Heartbeat(data);
            if( response != null )
                ProcessResponse(response);
        }

        private void ProcessResponse(HeartbeatResponse response)
        {
            switch( response.Command )
            {
            case DataServerCommand.ReportBlocks:
                _log.Info("Received ReportBlocks command.");
                _reportBlocks = true;
                break;
            }
        }
    }
}
