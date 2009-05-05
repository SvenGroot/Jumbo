using System;
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

        public JetHeartbeatResponse[] Heartbeat(Tkl.Jumbo.ServerAddress address, JetHeartbeatData[] data, int timeout)
        {
            return JobServer.Instance.Heartbeat(address, data, timeout);
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

        public CompletedTask[] WaitForTaskCompletion(Guid jobId, string[] tasks, int timeout)
        {
            return JobServer.Instance.WaitForTaskCompletion(jobId, tasks, timeout);
        }

        public JobStatus GetJobStatus(Guid jobId)
        {
            return JobServer.Instance.GetJobStatus(jobId);
        }

        public JetMetrics GetMetrics()
        {
            return JobServer.Instance.GetMetrics();
        }

        public string GetLogFileContents(int maxSize)
        {
            return JobServer.Instance.GetLogFileContents(maxSize);
        }

        #endregion
    }
}
