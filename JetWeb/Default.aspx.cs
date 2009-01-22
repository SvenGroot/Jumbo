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

        foreach( Guid jobId in metrics.RunningJobs )
        {
            JobStatus job = client.JobServer.GetJobStatus(jobId);
            HtmlTableRow row = new HtmlTableRow();
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"job.aspx?id={0}\">{{{0}}}</a>", jobId) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.StartTime.ToString(System.Globalization.DateTimeFormatInfo.InvariantInfo.UniversalSortableDateTimePattern) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.TaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.RunningTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.UnscheduledTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.FinishedTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.ErrorTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.NonDataLocalTaskCount.ToString() });
            RunningJobsTable.Rows.Add(row);
        }

        foreach( Guid jobId in metrics.FinishedJobs )
        {
            JobStatus job = client.JobServer.GetJobStatus(jobId);
            HtmlTableRow row = new HtmlTableRow();
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"job.aspx?id={0}\">{{{0}}}</a>", jobId) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.StartTime.ToString(System.Globalization.DateTimeFormatInfo.InvariantInfo.UniversalSortableDateTimePattern) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.EndTime.ToString(System.Globalization.DateTimeFormatInfo.InvariantInfo.UniversalSortableDateTimePattern) });
            TimeSpan duration = job.EndTime - job.StartTime;
            row.Cells.Add(new HtmlTableCell() { InnerText = string.Format("{0} ({1}s)", duration, duration.TotalSeconds) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.TaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.ErrorTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.NonDataLocalTaskCount.ToString() });
            FinishedJobsTable.Rows.Add(row);
        }

        foreach( Guid jobId in metrics.FailedJobs )
        {
            JobStatus job = client.JobServer.GetJobStatus(jobId);
            HtmlTableRow row = new HtmlTableRow();
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"job.aspx?id={0}\">{{{0}}}</a>", jobId) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.StartTime.ToString(System.Globalization.DateTimeFormatInfo.InvariantInfo.UniversalSortableDateTimePattern) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.EndTime.ToString() });
            TimeSpan duration = job.EndTime - job.StartTime;
            row.Cells.Add(new HtmlTableCell() { InnerText = string.Format("{0} ({1}s)", duration, duration.TotalSeconds) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.TaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.ErrorTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.NonDataLocalTaskCount.ToString() });
            FailedJobsTable.Rows.Add(row);
        }
    }
}
