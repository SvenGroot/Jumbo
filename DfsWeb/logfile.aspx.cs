// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Tkl.Jumbo;
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
            maxSize = (int)ByteSize.Parse(maxSizeString);
        if( maxSize <= 0 )
            maxSize = Int32.MaxValue;
        if( dataServer == null )
        {
            DfsClient client = new DfsClient();
            DfsMetrics metrics = client.NameServer.GetMetrics();
            Title = string.Format("Name server {0} log file - Jumbo DFS", metrics.NameServer);
            HeaderText.InnerText = string.Format("Name server {0} log file", metrics.NameServer);
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
