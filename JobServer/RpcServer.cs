// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo;

namespace JobServerApplication
{
    class RpcServer : MarshalByRefObject, IJobServerHeartbeatProtocol, IJobServerClientProtocol, IJobServerTaskProtocol
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

        public bool AbortJob(Guid jobId)
        {
            return JobServer.Instance.AbortJob(jobId);
        }
        
        public ServerAddress GetTaskServerForTask(Guid jobID, string taskID)
        {
            return JobServer.Instance.GetTaskServerForTask(jobID, taskID);
        }

        public CompletedTask[] CheckTaskCompletion(Guid jobId, string[] tasks)
        {
            return JobServer.Instance.CheckTaskCompletion(jobId, tasks);
        }

        public JobStatus GetJobStatus(Guid jobId)
        {
            return JobServer.Instance.GetJobStatus(jobId);
        }

        public JobStatus[] GetRunningJobs()
        {
            return JobServer.Instance.GetRunningJobs();
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

        #region IJobServerTaskProtocol Members

        public int[] GetPartitionsForTask(Guid jobId, TaskId taskId)
        {
            return JobServer.Instance.GetPartitionsForTask(jobId, taskId);
        }

        public int[] GetAdditionalPartitions(Guid jobId, TaskId taskId)
        {
            return JobServer.Instance.GetAdditionalPartitions(jobId, taskId);
        }

        public bool NotifyStartPartitionProcessing(Guid jobId, TaskId taskId, int partitionNumber)
        {
            return JobServer.Instance.NotifyStartPartitionProcessing(jobId, taskId, partitionNumber);
        }

        #endregion
    }
}
