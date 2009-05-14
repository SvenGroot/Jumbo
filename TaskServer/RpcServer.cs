﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;

namespace TaskServerApplication
{
    class RpcServer : MarshalByRefObject, ITaskServerUmbilicalProtocol, ITaskServerClientProtocol
    {
        #region ITaskServerUmbilicalProtocol Members

        public void ReportCompletion(Guid jobID, string taskID)
        {
            TaskServer.Instance.ReportCompletion(jobID, taskID);
        }

        public void ReportProgress(Guid jobId, string taskId, float progress)
        {
            TaskServer.Instance.ReportProgress(jobId, taskId, progress);
        }

        public void SetUncompressedTemporaryFileSize(Guid jobId, string fileName, long uncompressedSize)
        {
            TaskServer.Instance.SetUncompressedTemporaryFileSize(jobId, fileName, uncompressedSize);
        }

        public long GetUncompressedTemporaryFileSize(Guid jobId, string fileName)
        {
            return TaskServer.Instance.GetUncompressedTemporaryFileSize(jobId, fileName);
        }

        #endregion

        #region ITaskServerClientProtocol Members

        public int FileServerPort
        {
            get { return TaskServer.Instance.FileServerPort; }
        }

        public TaskAttemptStatus GetTaskStatus(string fullTaskID)
        {
            return TaskServer.Instance.GetTaskStatus(fullTaskID);
        }

        public string GetOutputFileDirectory(Guid jobId, string taskId)
        {
            return TaskServer.Instance.GetOutputFileDirectory(jobId, taskId);
        }

        public string GetLogFileContents(int maxSize)
        {
            return TaskServer.Instance.GetLogFileContents(maxSize);
        }

        public byte[] GetCompressedTaskLogFiles(Guid jobId)
        {
            return TaskServer.Instance.GetCompressedTaskLogFiles(jobId);
        }

        public string GetTaskLogFileContents(Guid jobId, string taskId, int attempt, int maxSize)
        {
            return TaskServer.Instance.GetTaskLogFileContents(jobId, taskId, attempt, maxSize);
        }

        public string GetTaskProfileOutput(Guid jobId, string taskId, int attempt)
        {
            return TaskServer.Instance.GetTaskProfileOutput(jobId, taskId, attempt);
        }

        #endregion
    }
}
