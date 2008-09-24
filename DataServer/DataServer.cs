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
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(DataServer));
        private INameServerHeartbeatProtocol _nameServer;
        private const int _heartbeatInterval = 2000;

        public DataServer(INameServerHeartbeatProtocol nameServer)
        {
            if( nameServer == null )
                throw new ArgumentNullException("nameServer");

            _nameServer = nameServer;
        }

        public void Run()
        {
            _log.Info("Data server main loop starting.");
            while( true )
            {
                SendHeartbeat();
                Thread.Sleep(_heartbeatInterval);
            }
        }

        private void SendHeartbeat()
        {
            //_log.Debug("Sending heartbeat to name server.");
            HeartbeatResponse response = _nameServer.Heartbeat(new HeartbeatData());
            if( response != null )
                ProcessResponse(response);
        }

        private void ProcessResponse(HeartbeatResponse response)
        {
            switch( response.Command )
            {
            case DataServerCommand.ReportBlocks:
                _log.Info("Received ReportBlocks command: sending blocks to server.");
                HeartbeatData data = new HeartbeatData();
                data.Flags = HeartbeatFlags.BlockReport;
                data.Blocks = new List<Guid>(); // TODO: Real block report

                // Should we do this immediately or rather wait until the next normal heartbeat. I don't know.
                _nameServer.Heartbeat(data);
                break;
            }
        }
    }
}
