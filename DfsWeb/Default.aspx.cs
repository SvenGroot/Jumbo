﻿using System;
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
        JumboVersionLabel.Text = typeof(DfsClient).Assembly.GetName().Version.ToString();
        OsVersionLabel.Text = Environment.OSVersion.ToString();
        ClrVersionLabel.Text = RuntimeEnvironment.Description;
        ArchitectureLabel.Text = (IntPtr.Size * 8).ToString();

        DfsClient client = new DfsClient();
        DfsMetrics metrics = client.NameServer.GetMetrics();
        TotalSizeColumn.InnerHtml = FormatSize(metrics.TotalSize);
        BlocksColumn.InnerText = metrics.TotalBlockCount.ToString();
        UnderReplicatedBlocksColumn.InnerText = metrics.UnderReplicatedBlockCount.ToString();
        PendingBlocksColumn.InnerText = metrics.PendingBlockCount.ToString();
        DataServersColumn.InnerText = metrics.DataServers.Length.ToString();
        SafeModeColumn.InnerText = client.NameServer.SafeMode ? "ON" : "OFF";

        foreach( DataServerMetrics server in metrics.DataServers )
        {
            HtmlTableRow row = new HtmlTableRow();
            row.Cells.Add(new HtmlTableCell() { InnerText = server.Address.HostName });
            row.Cells.Add(new HtmlTableCell() { InnerText = server.Address.Port.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = string.Format("{0:0.0}s ago", (DateTime.UtcNow - server.LastContactUtc).TotalSeconds) });
            row.Cells.Add(new HtmlTableCell() { InnerText = server.BlockCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("Used: {0} / Free: {1}", FormatSize(server.DiskSpaceUsed), FormatSize(server.DiskSpaceFree)) });
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
