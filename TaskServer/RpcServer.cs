using System;
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

        #endregion

        #region ITaskServerClientProtocol Members

        public int FileServerPort
        {
            get { return TaskServer.Instance.FileServerPort; }
        }

        public TaskStatus GetTaskStatus(string fullTaskID)
        {
            return TaskServer.Instance.GetTaskStatus(fullTaskID);
        }

        public string GetOutputFileDirectory(string fullTaskID)
        {
            return TaskServer.Instance.GetOutputFileDirectory(fullTaskID);
        }

        #endregion
    }
}
