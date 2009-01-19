﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo;

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

        public bool WaitForJobCompletion(Guid jobID, int timeout)
        {
            return JobServer.Instance.WaitForJobCompletion(jobID, timeout);
        }
        
        public ServerAddress GetTaskServerForTask(Guid jobID, string taskID)
        {
            return JobServer.Instance.GetTaskServerForTask(jobID, taskID);
        }

        public JobStatus GetJobStatus(Guid jobId)
        {
            return JobServer.Instance.GetJobStatus(jobId);
        }

        public JetMetrics GetMetrics()
        {
            return JobServer.Instance.GetMetrics();
        }

        #endregion
    }
}
