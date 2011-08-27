// $Id$
//
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
        Title = string.Format("Jumbo DFS ({0})", metrics.NameServer);
        NameServerColumn.InnerText = metrics.NameServer.ToString();
        TotalSizeColumn.InnerHtml = FormatSize(metrics.TotalSize);
        TotalCapacityColumn.InnerHtml = FormatSize(metrics.TotalCapacity);
        DfsCapacityUsedColumn.InnerHtml = FormatSize(metrics.DfsCapacityUsed);
        AvailableCapacityColumn.InnerHtml = FormatSize(metrics.AvailableCapacity);
        BlocksColumn.InnerHtml = string.Format("<a href=\"blocklist.aspx\">{0}</a>", metrics.TotalBlockCount);
        UnderReplicatedBlocksColumn.InnerHtml = string.Format("<a href=\"blocklist.aspx?kind=UnderReplicated\">{0}</a>", metrics.UnderReplicatedBlockCount);
        PendingBlocksColumn.InnerHtml = string.Format("<a href=\"blocklist.aspx?kind=Pending\">{0}</a>", metrics.PendingBlockCount);
        DataServersColumn.InnerText = metrics.DataServers.Count.ToString();
        bool safeMode = client.NameServer.SafeMode;
        SafeModeColumn.InnerHtml = string.Format("<a href=\"setsafemode.aspx?safeMode={0}\">{1}</a>", (!safeMode).ToString(), safeMode ? "ON" : "OFF");

        foreach( DataServerMetrics server in metrics.DataServers.OrderBy((s) => s.Address) )
        {
            HtmlTableRow row = new HtmlTableRow();
            TimeSpan lastContact = DateTime.UtcNow - server.LastContactUtc;
            if( lastContact.TotalSeconds > 60 )
                row.BgColor = "red";
            else if( lastContact.TotalSeconds > 5 )
                row.BgColor = "yellow";
            row.Cells.Add(new HtmlTableCell() { InnerText = server.Address.HostName });
            row.Cells.Add(new HtmlTableCell() { InnerText = server.RackId });
            row.Cells.Add(new HtmlTableCell() { InnerText = server.Address.Port.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = string.Format("{0:0.0}s ago", lastContact.TotalSeconds) });
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"blocklist.aspx?dataServer={0}&amp;port={1}\">{2}</a>", Server.HtmlEncode(server.Address.HostName), server.Address.Port, server.BlockCount) });
            HtmlTableCell diskSpaceCell = new HtmlTableCell() { InnerHtml = string.Format("Total: {0} / Used: {1} / Free: {2}", FormatSize(server.DiskSpaceTotal), FormatSize(server.DiskSpaceUsed), FormatSize(server.DiskSpaceFree)) };
            if( server.DiskSpaceFree < DfsConfiguration.GetConfiguration().NameServer.DataServerFreeSpaceThreshold )
                diskSpaceCell.Style.Add(HtmlTextWriterStyle.BackgroundColor, "yellow");
            row.Cells.Add(diskSpaceCell);
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"logfile.aspx?dataServer={0}&amp;port={1}&amp;maxSize=100KB\">Last 100KB</a>, <a href=\"logfile.aspx?dataServer={0}&amp;port={1}&amp;maxSize=0\">all</a>", Server.HtmlEncode(server.Address.HostName), server.Address.Port) });
            DataServerTable.Rows.Add(row);
        }
    }

    private string FormatSize(long bytes)
    {
        if( bytes < BinarySize.Kilobyte )
        {
            return string.Format("{0:#,0} bytes", bytes);
        }
        return string.Format("<abbr title=\"{1:#,0} bytes\">{0:#,0.# SB}</abbr>", (BinarySize)bytes, bytes);
    }
}
