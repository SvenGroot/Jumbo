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

        public void ReportCompletion(string fullTaskID)
        {
            TaskServer.Instance.ReportCompletion(fullTaskID);
        }

        #endregion

        #region ITaskServerClientProtocol Members

        public TaskStatus GetTaskStatus(string fullTaskID)
        {
            return TaskServer.Instance.GetTaskStatus(fullTaskID);
        }

        #endregion
    }
}
