using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;

namespace JobServerApplication
{
    class RpcServer : MarshalByRefObject, IJobServerHeartbeatProtocol, IJobServerClientProtocol
    {
        #region IJobServerHeartbeatProtocol Members

        public JetHeartbeatResponse[] Heartbeat(Tkl.Jumbo.ServerAddress address, JetHeartbeatData[] data)
        {
            return JobServer.Instance.Heartbeat(address, data);
        }

        #endregion

        #region IJobServerClientProtocol Members

        public Job CreateJob()
        {
            return JobServer.Instance.CreateJob();
        }

        public void RunJob(Guid jobID)
        {
            JobServer.Instance.RunJob(jobID);
        }

        #endregion
    }
}
