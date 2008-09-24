using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;

namespace DataServer
{
    class DataServer
    {
        private INameServerHeartbeatProtocol _nameServer;

        public DataServer(INameServerHeartbeatProtocol nameServer)
        {
            if( _nameServer == null )
                throw new ArgumentNullException("nameServer");

            _nameServer = nameServer;
        }

        private void SendHeartbeat()
        {
            
        }
    }
}
