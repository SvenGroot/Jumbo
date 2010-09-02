// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo;

namespace TaskServerApplication
{
    class RpcServer : MarshalByRefObject, ITaskServerUmbilicalProtocol, ITaskServerClientProtocol
    {
        #region ITaskServerUmbilicalProtocol Members

        public void ReportCompletion(Guid jobID, TaskAttemptId taskAttemptId, TaskMetrics metrics)
        {
            TaskServer.Instance.ReportCompletion(jobID, taskAttemptId, metrics);
        }

        public void ReportProgress(Guid jobId, TaskAttemptId taskAttemptId, TaskProgress progress)
        {
            TaskServer.Instance.ReportProgress(jobId, taskAttemptId, progress);
        }

        public void SetUncompressedTemporaryFileSize(Guid jobId, string fileName, long uncompressedSize)
        {
            TaskServer.Instance.SetUncompressedTemporaryFileSize(jobId, fileName, uncompressedSize);
        }

        public long GetUncompressedTemporaryFileSize(Guid jobId, string fileName)
        {
            return TaskServer.Instance.GetUncompressedTemporaryFileSize(jobId, fileName);
        }

        public void RegisterTcpChannelPort(Guid jobId, TaskAttemptId taskAttemptId, int port)
        {
            TaskServer.Instance.RegisterTcpChannelPort(jobId, taskAttemptId, port);
        }

        public string DownloadDfsFile(Guid jobId, string dfsPath)
        {
            return TaskServer.Instance.DownloadDfsFile(jobId, dfsPath);
        }

        #endregion

        #region ITaskServerClientProtocol Members

        public int FileServerPort
        {
            get { return TaskServer.Instance.FileServerPort; }
        }

        public TaskAttemptStatus GetTaskStatus(Guid jobId, TaskAttemptId taskAttemptId)
        {
            return TaskServer.Instance.GetTaskStatus(jobId, taskAttemptId);
        }

        public string GetOutputFileDirectory(Guid jobId)
        {
            return TaskServer.Instance.GetOutputFileDirectory(jobId);
        }

        public string GetLogFileContents(LogFileKind kind, int maxSize)
        {
            return TaskServer.Instance.GetLogFileContents(kind, maxSize);
        }

        public byte[] GetCompressedTaskLogFiles(Guid jobId)
        {
            return TaskServer.Instance.GetCompressedTaskLogFiles(jobId);
        }

        public string GetTaskLogFileContents(Guid jobId, TaskAttemptId taskAttemptId, int maxSize)
        {
            return TaskServer.Instance.GetTaskLogFileContents(jobId, taskAttemptId, maxSize);
        }

        public string GetTaskProfileOutput(Guid jobId, TaskAttemptId taskAttemptId)
        {
            return TaskServer.Instance.GetTaskProfileOutput(jobId, taskAttemptId);
        }

        public int GetTcpChannelPort(Guid jobId, TaskAttemptId taskAttemptId)
        {
            return TaskServer.Instance.GetTcpChannelPort(jobId, taskAttemptId);
        }

        #endregion
    }
}
