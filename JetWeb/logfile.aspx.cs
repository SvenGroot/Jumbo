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
        if( taskServer == null )
        {
            Title = "Job server log file - Jumbo DFS";
            HeaderText.InnerText = "Job server log file";
            JetClient client = new JetClient();
            string log = client.JobServer.GetLogFileContents();
            LogFileContents.InnerText = log;
        }
        else
        {
            int port = Convert.ToInt32(Request.QueryString["port"]);
            ITaskServerClientProtocol client = JetClient.CreateTaskServerClient(new ServerAddress(taskServer, port));
            LogFileContents.InnerText = client.GetLogFileContents();
            Title = string.Format("Data server {0} log file - Jumbo DFS", taskServer);
            HeaderText.InnerText = string.Format("Data server {0} log file", taskServer);
        }
    }
}
