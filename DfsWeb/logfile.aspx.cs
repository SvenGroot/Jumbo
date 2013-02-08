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
using Tkl.Jumbo.Dfs.FileSystem;
using System.Text;

public partial class logfile : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {
        string dataServer = Request.QueryString["dataServer"];
        string maxSizeString = Request.QueryString["maxSize"];
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

        int maxSize = 102400;
        if( maxSizeString != null )
            maxSize = (int)BinarySize.Parse(maxSizeString);
        if( maxSize <= 0 )
            maxSize = Int32.MaxValue;
        if( dataServer == null )
        {
            DfsClient client = (DfsClient)FileSystemClient.Create();
            DfsMetrics metrics = client.NameServer.GetMetrics();
            Title = string.Format("Name server {0} log file - Jumbo DFS", metrics.NameServer);
            HeaderText.InnerText = string.Format("Name server {0} log file", metrics.NameServer);
            string log = client.NameServer.GetLogFileContents(kind, maxSize);
            LogFileContents.InnerHtml = FormatLogFile(log);
        }
        else
        {
            int port = Convert.ToInt32(Request.QueryString["port"]);
            LogFileContents.InnerHtml = FormatLogFile(DfsClient.GetDataServerLogFileContents(dataServer, port, kind, maxSize));
            Title = string.Format("Data server {0} log file - Jumbo DFS", dataServer);
            HeaderText.InnerText = string.Format("Data server {0} log file", dataServer);
        }
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
                    result.AppendFormat("<span class=\"warning\">{0}</span>", Server.HtmlEncode(line));
                else if( line.Contains(" ERROR ") )
                    result.AppendFormat("<span class=\"error\">{0}</span>", Server.HtmlEncode(line));
                else
                    result.Append(Server.HtmlEncode(line));
                result.AppendLine();
            }
        }

        return result.ToString();
    }
}
