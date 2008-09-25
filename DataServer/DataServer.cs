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

        private INameServerHeartbeatProtocol _nameServer;
        private bool _reportBlocks = false;

        private List<Guid> _blocks = new List<Guid>();
        private BlockServer _blockServer; // listens for TCP connections.

        public DataServer(INameServerHeartbeatProtocol nameServer)
        {
            if( nameServer == null )
                throw new ArgumentNullException("nameServer");

            _nameServer = nameServer;
        }

        public void Run()
        {
            _log.Info("Data server main loop starting.");
            _blockServer = new BlockServer(this);
            _blockServer.RunAsync();
            while( true )
            {
                SendHeartbeat();
                Thread.Sleep(_heartbeatInterval);
            }
        }

        public FileStream AddNewBlock(Guid blockID)
        {
            lock( _blocks )
            {
                if( _blocks.Contains(blockID) )
                    throw new ArgumentException("Existing block ID.");
                _blocks.Add(blockID);
                return System.IO.File.Create(Path.Combine(_blockStorageDirectory, blockID.ToString()));
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
                lock( _blocks )
                {
                    data = new BlockReportData() { Blocks = _blocks.ToArray() };
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
                _reportBlocks = true;
                break;
            }
        }
    }
}
