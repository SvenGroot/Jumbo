using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.IO;
using Tkl.Jumbo;
using Tkl.Jumbo.Jet;
using System.Web.UI.HtmlControls;

public partial class _Default : System.Web.UI.Page 
{
    protected void Page_Load(object sender, EventArgs e)
    {
        JetClient client = new JetClient();
        JetMetrics metrics = client.JobServer.GetMetrics();
        RunningJobsColumn.InnerText = metrics.RunningJobs.Length.ToString();
        FinishedJobsColumn.InnerText = metrics.FinishedJobs.Length.ToString();
        FailedJobsColumn.InnerText = metrics.FailedJobs.Length.ToString();
        CapacityColumn.InnerText = metrics.Capacity.ToString();
        NonInputTaskCapacityColumn.InnerText = metrics.NonInputTaskCapacity.ToString();
        SchedulerColumn.InnerText = metrics.Scheduler;
        TaskServersColumn.InnerText = metrics.TaskServers.Length.ToString();

        foreach( TaskServerMetrics server in metrics.TaskServers )
        {
            HtmlTableRow row = new HtmlTableRow();
            row.Cells.Add(new HtmlTableCell() { InnerText = server.Address.HostName });
            row.Cells.Add(new HtmlTableCell() { InnerText = server.Address.Port.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = string.Format("{0:0.0}s ago", (DateTime.UtcNow - server.LastContactUtc).TotalSeconds) });
            row.Cells.Add(new HtmlTableCell() { InnerText = server.MaxTasks.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = server.MaxNonInputTasks.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"logfile.aspx?taskServer={0}&amp;port={1}\">View</a>", Server.HtmlEncode(server.Address.HostName), server.Address.Port) });
            DataServerTable.Rows.Add(row);
        }
    }
}
