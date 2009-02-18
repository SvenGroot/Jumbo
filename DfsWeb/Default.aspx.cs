using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Tkl.Jumbo.Dfs;
using System.IO;
using Tkl.Jumbo;
using System.Web.UI.HtmlControls;

public partial class _Default : System.Web.UI.Page 
{
    protected void Page_Load(object sender, EventArgs e)
    {
        DfsClient client = new DfsClient();
        DfsMetrics metrics = client.NameServer.GetMetrics();
        TotalSizeColumn.InnerHtml = FormatSize(metrics.TotalSize);
        BlocksColumn.InnerText = metrics.TotalBlockCount.ToString();
        UnderReplicatedBlocksColumn.InnerText = metrics.UnderReplicatedBlockCount.ToString();
        PendingBlocksColumn.InnerText = metrics.PendingBlockCount.ToString();
        DataServersColumn.InnerText = metrics.DataServers.Length.ToString();
        bool safeMode = client.NameServer.SafeMode;
        SafeModeColumn.InnerHtml = string.Format("<a href=\"setsafemode.aspx?safeMode={0}\">{1}</a>", (!safeMode).ToString(), safeMode ? "ON" : "OFF");

        foreach( DataServerMetrics server in metrics.DataServers )
        {
            HtmlTableRow row = new HtmlTableRow();
            row.Cells.Add(new HtmlTableCell() { InnerText = server.Address.HostName });
            row.Cells.Add(new HtmlTableCell() { InnerText = server.Address.Port.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = string.Format("{0:0.0}s ago", (DateTime.UtcNow - server.LastContactUtc).TotalSeconds) });
            row.Cells.Add(new HtmlTableCell() { InnerText = server.BlockCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("Used: {0} / Free: {1}", FormatSize(server.DiskSpaceUsed), FormatSize(server.DiskSpaceFree)) });
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"logfile.aspx?dataServer={0}&amp;port={1}\">View</a>", Server.HtmlEncode(server.Address.HostName), server.Address.Port) });
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"blocklist.aspx?dataServer={0}&amp;port={1}\">View</a>", Server.HtmlEncode(server.Address.HostName), server.Address.Port) });
            DataServerTable.Rows.Add(row);
        }
    }

    private string FormatSize(long bytes)
    {
        double size;
        string unit;
        if( bytes > 0x40000000 )
        {
            size = bytes / (double)0x40000000;
            unit = "GB";
        }
        else if( bytes > 0x100000 )
        {
            size = bytes / (double)0x100000;
            unit = "MB";
        }
        else if( bytes > 0x400 )
        {
            size = bytes / (double)0x400;
            unit = "KB";
        }
        else
        {
            return string.Format("{0:#,0} bytes", bytes);
        }
        return string.Format("<abbr title=\"{2:#,0} bytes\">{0:#,0.0} {1}</abbr>", size, unit, bytes);
    }
}
