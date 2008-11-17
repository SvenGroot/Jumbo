using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Interface used by the TaskHost to communicate with its task server.
    /// </summary>
    public interface ITaskServerUmbilicalProtocol
    {
        /// <summary>
        /// Reports successful task completion to the task server.
        /// </summary>
        /// <param name="fullTaskID">The full ID of the task.</param>
        void ReportCompletion(string fullTaskID);
    }
}
