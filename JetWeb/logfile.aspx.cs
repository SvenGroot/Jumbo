﻿using System;
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
            maxSize = Convert.ToInt32(maxSizeString);
        if( taskServer == null )
        {
            Title = "Job server log file - Jumbo Jet";
            HeaderText.InnerText = "Job server log file";
            JetClient client = new JetClient();
            string log = client.JobServer.GetLogFileContents(maxSize);
            LogFileContents.InnerText = log;
        }
        else
        {
            int port = Convert.ToInt32(Request.QueryString["port"]);
            ITaskServerClientProtocol client = JetClient.CreateTaskServerClient(new ServerAddress(taskServer, port));

            string taskId = Request.QueryString["task"];
            if( taskId == null )
            {
                LogFileContents.InnerText = client.GetLogFileContents(maxSize);
                Title = string.Format("Task server {0} log file - Jumbo Jet", taskServer);
                HeaderText.InnerText = string.Format("Data server {0} log file", taskServer);
            }
            else
            {
                Guid jobId = new Guid(Request.QueryString["job"]);
                int attempt = Convert.ToInt32(Request.QueryString["attempt"]);

                if( Request.QueryString["profile"] == "true" )
                {
                    LogFileContents.InnerText = client.GetTaskProfileOutput(jobId, taskId, attempt);
                    Title = string.Format("Task {{{0}}}_{1}_{2} profile output (on {3}) - Jumbo Jet", jobId, taskId, attempt, taskServer);
                    HeaderText.InnerText = string.Format("Task {{{0}}}_{1}_{2} profile output (on {3})", jobId, taskId, attempt, taskServer);
                }
                else
                {
                    LogFileContents.InnerText = client.GetTaskLogFileContents(jobId, taskId, attempt, maxSize);
                    Title = string.Format("Task {{{0}}}_{1}_{2} log file (on {3}) - Jumbo Jet", jobId, taskId, attempt, taskServer);
                    HeaderText.InnerText = string.Format("Task {{{0}}}_{1}_{2} log file (on {3})", jobId, taskId, attempt, taskServer);
                }
            }
        }
    }
}