// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Tkl.Jumbo.Jet;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using Tkl.Jumbo;

public partial class logfile : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {
        string taskServer = Request.QueryString["taskServer"];
        string maxSizeString = Request.QueryString["maxSize"];
        int maxSize = 102400;
        if( maxSizeString != null )
            maxSize = (int)ByteSize.Parse(maxSizeString);
        if( maxSize <= 0 )
            maxSize = Int32.MaxValue;

        string kindString = Request.QueryString["kind"];
        LogFileKind kind;
        switch( kindString )
        {
        case "out":
            kind = LogFileKind.StdOut;
            break;
        case "err":
            kind = LogFileKind.StdErr;
            break;
        default:
            kind = LogFileKind.Log;
            break;
        }


        if( taskServer == null )
        {
            JetClient client = new JetClient();
            JetMetrics metrics = client.JobServer.GetMetrics();
            Title = string.Format("Job server {0} log file - Jumbo Jet", metrics.JobServer);
            HeaderText.InnerText = string.Format("Job server {0} log file", metrics.JobServer);
            string log = client.JobServer.GetLogFileContents(kind, maxSize);
            LogFileContents.InnerText = log;
        }
        else
        {
            int port = Convert.ToInt32(Request.QueryString["port"]);
            ITaskServerClientProtocol client = JetClient.CreateTaskServerClient(new ServerAddress(taskServer, port));

            string taskId = Request.QueryString["task"];
            if( taskId == null )
            {
                LogFileContents.InnerText = client.GetLogFileContents(kind, maxSize);
                Title = string.Format("Task server {0} log file - Jumbo Jet", taskServer);
                HeaderText.InnerText = string.Format("Task server {0} log file", taskServer);
            }
            else
            {
                Guid jobId = new Guid(Request.QueryString["job"]);
                int attempt = Convert.ToInt32(Request.QueryString["attempt"]);

                if( Request.QueryString["profile"] == "true" )
                {
                    LogFileContents.InnerText = client.GetTaskProfileOutput(jobId, new TaskAttemptId(new TaskId(taskId), attempt));
                    Title = string.Format("Task {{{0}}}_{1}_{2} profile output (on {3}) - Jumbo Jet", jobId, taskId, attempt, taskServer);
                    HeaderText.InnerText = string.Format("Task {{{0}}}_{1}_{2} profile output (on {3})", jobId, taskId, attempt, taskServer);
                }
                else
                {
                    LogFileContents.InnerText = client.GetTaskLogFileContents(jobId, new TaskAttemptId(new TaskId(taskId), attempt), maxSize);
                    Title = string.Format("Task {{{0}}}_{1}_{2} log file (on {3}) - Jumbo Jet", jobId, taskId, attempt, taskServer);
                    HeaderText.InnerText = string.Format("Task {{{0}}}_{1}_{2} log file (on {3})", jobId, taskId, attempt, taskServer);
                }
            }
        }
    }
}
