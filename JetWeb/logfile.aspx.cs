// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Ookii.Jumbo.Jet;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using Ookii.Jumbo;
using System.Text;

public partial class logfile : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {
        string taskServer = Request.QueryString["taskServer"];
        string maxSizeString = Request.QueryString["maxSize"];
        int maxSize = 102400;
        if( maxSizeString != null )
            maxSize = (int)BinarySize.Parse(maxSizeString);
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

        string log;
        if( taskServer == null )
        {
            JetClient client = new JetClient();
            JetMetrics metrics = client.JobServer.GetMetrics();
            Title = string.Format("Job server {0} log file - Jumbo Jet", metrics.JobServer);
            HeaderText.InnerText = string.Format("Job server {0} log file", metrics.JobServer);
            log = client.JobServer.GetLogFileContents(kind, maxSize);
        }
        else
        {
            int port = Convert.ToInt32(Request.QueryString["port"]);
            ITaskServerClientProtocol client = JetClient.CreateTaskServerClient(new ServerAddress(taskServer, port));

            string taskId = Request.QueryString["task"];
            if( taskId == null )
            {
                log = client.GetLogFileContents(kind, maxSize);
                Title = string.Format("Task server {0} log file - Jumbo Jet", taskServer);
                HeaderText.InnerText = string.Format("Task server {0} log file", taskServer);
            }
            else
            {
                Guid jobId = new Guid(Request.QueryString["job"]);
                int attempt = Convert.ToInt32(Request.QueryString["attempt"]);

                if( Request.QueryString["profile"] == "true" )
                {
                    log = client.GetTaskProfileOutput(jobId, new TaskAttemptId(new TaskId(taskId), attempt));
                    Title = string.Format("Task {{{0}}}_{1}_{2} profile output (on {3}) - Jumbo Jet", jobId, taskId, attempt, taskServer);
                    HeaderText.InnerText = string.Format("Task {{{0}}}_{1}_{2} profile output (on {3})", jobId, taskId, attempt, taskServer);
                }
                else
                {
                    log = client.GetTaskLogFileContents(jobId, new TaskAttemptId(new TaskId(taskId), attempt), maxSize);
                    Title = string.Format("Task {{{0}}}_{1}_{2} log file (on {3}) - Jumbo Jet", jobId, taskId, attempt, taskServer);
                    HeaderText.InnerText = string.Format("Task {{{0}}}_{1}_{2} log file (on {3})", jobId, taskId, attempt, taskServer);
                }
            }
        }
        LogFileContents.InnerHtml = FormatLogFile(log);
    }

    private string FormatLogFile(string log)
    {
        StringBuilder result = new StringBuilder(log.Length);
        using( StringReader reader = new StringReader(log) )
        {
            string line;
            while( (line = reader.ReadLine()) != null )
            {
                if( line.Contains(" WARN ") )
                    result.AppendFormat("<span class=\"logWarning\">{0}</span>", Server.HtmlEncode(line));
                else if( line.Contains(" ERROR ") )
                    result.AppendFormat("<span class=\"logError\">{0}</span>", Server.HtmlEncode(line));
                else
                    result.Append(Server.HtmlEncode(line));
                result.AppendLine();
            }
        }

        return result.ToString();
    }
}
