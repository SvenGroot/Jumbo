using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Tkl.Jumbo.Dfs;
using System.IO;
using Tkl.Jumbo;

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
        TotalSizeColumn.InnerText = string.Format("{0:#,0} bytes", metrics.TotalSize);
        BlocksColumn.InnerText = metrics.TotalBlockCount.ToString();
        UnderReplicatedBlocksColumn.InnerText = metrics.UnderReplicatedBlockCount.ToString();
        PendingBlocksColumn.InnerText = metrics.PendingBlockCount.ToString();
        DataServersColumn.InnerText = metrics.DataServers.Length.ToString();
        SafeModeColumn.InnerText = client.NameServer.SafeMode ? "ON" : "OFF";
    }
}
