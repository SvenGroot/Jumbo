using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Tkl.Jumbo.Dfs;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;

public partial class logfile : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {
        string dataServer = Request.QueryString["dataServer"];
        string maxSizeString = Request.QueryString["maxSize"];
        int maxSize = 102400;
        if( maxSizeString != null )
            maxSize = Convert.ToInt32(maxSizeString);
        if( dataServer == null )
        {
            Title = "Name server log file - Jumbo DFS";
            HeaderText.InnerText = "Name server log file";
            DfsClient client = new DfsClient();
            string log = client.NameServer.GetLogFileContents(maxSize);
            LogFileContents.InnerText = log;
        }
        else
        {
            int port = Convert.ToInt32(Request.QueryString["port"]);
            LogFileContents.InnerText = DfsClient.GetDataServerLogFileContents(dataServer, port, maxSize);
            Title = string.Format("Data server {0} log file - Jumbo DFS", dataServer);
            HeaderText.InnerText = string.Format("Data server {0} log file", dataServer);
        }
    }
}
