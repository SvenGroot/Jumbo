// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.IO;
using Ookii.Jumbo;
using Ookii.Jumbo.Jet;
using System.Web.UI.HtmlControls;

public partial class _Default : System.Web.UI.Page 
{
    protected void Page_Load(object sender, EventArgs e)
    {
        JetClient client = new JetClient();
        JetMetrics metrics = client.JobServer.GetMetrics();
        Title = string.Format("Jumbo Jet ({0})", metrics.JobServer);
        JobServerColumn.InnerText = metrics.JobServer.ToString();
        RunningJobsColumn.InnerText = metrics.RunningJobs.Count.ToString();
        FinishedJobsColumn.InnerText = metrics.FinishedJobs.Count.ToString();
        FailedJobsColumn.InnerText = metrics.FailedJobs.Count.ToString();
        CapacityColumn.InnerText = metrics.Capacity.ToString();
        SchedulerColumn.InnerText = metrics.Scheduler;
        TaskServersColumn.InnerText = metrics.TaskServers.Count.ToString();

        foreach( TaskServerMetrics server in metrics.TaskServers.OrderBy(s => s.Address) )
        {
            HtmlTableRow row = new HtmlTableRow();
            TimeSpan lastContact = DateTime.UtcNow - server.LastContactUtc;
            if( lastContact.TotalSeconds > 60 )
                row.BgColor = "red";
            else if( lastContact.TotalSeconds > 5 )
                row.BgColor = "yellow";
            row.Cells.Add(new HtmlTableCell() { InnerText = server.Address.HostName });
            row.Cells.Add(new HtmlTableCell() { InnerText = server.Address.Port.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = server.RackId ?? "(unknown)" });
            row.Cells.Add(new HtmlTableCell() { InnerText = string.Format("{0:0.0}s ago", lastContact.TotalSeconds) });
            row.Cells.Add(new HtmlTableCell() { InnerText = server.TaskSlots.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"logfile.aspx?taskServer={0}&amp;port={1}&amp;maxSize=100KB\">Last 100KB</a>, <a href=\"logfile.aspx?taskServer={0}&amp;port={1}&amp;maxSize=0\">all</a>", Server.HtmlEncode(server.Address.HostName), server.Address.Port) });
            DataServerTable.Rows.Add(row);
        }

        foreach( Guid jobId in metrics.RunningJobs )
        {
            JobStatus job = client.JobServer.GetJobStatus(jobId);
            HtmlTableRow row = new HtmlTableRow();
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"job.aspx?id={0}&amp;refresh=5\">{{{0}}}</a>", jobId) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.JobName ?? "(unnamed)" });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.StartTime.ToString(System.Globalization.DateTimeFormatInfo.InvariantInfo.UniversalSortableDateTimePattern) });
            TimeSpan duration = DateTime.UtcNow - job.StartTime;
            row.Cells.Add(new HtmlTableCell() { InnerText = string.Format(@"{0:hh\:mm\:ss\.ff} ({1:0.00}s)", duration, duration.TotalSeconds) });
            row.Cells.Add(CreateProgressCell(job.Progress));
            row.Cells.Add(new HtmlTableCell() { InnerText = job.TaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.RunningTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.UnscheduledTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.FinishedTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.ErrorTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.RackLocalTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.NonDataLocalTaskCount.ToString() });
            RunningJobsTable.Rows.Add(row);
        }

        foreach( Guid jobId in metrics.FinishedJobs )
        {
            JobStatus job = client.JobServer.GetJobStatus(jobId);
            HtmlTableRow row = new HtmlTableRow();
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"job.aspx?id={0}\">{{{0}}}</a>", jobId) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.JobName ?? "(unnamed)" });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.StartTime.ToString(System.Globalization.DateTimeFormatInfo.InvariantInfo.UniversalSortableDateTimePattern) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.EndTime.ToString(System.Globalization.DateTimeFormatInfo.InvariantInfo.UniversalSortableDateTimePattern) });
            TimeSpan duration = job.EndTime - job.StartTime;
            row.Cells.Add(new HtmlTableCell() { InnerText = string.Format(@"{0:hh\:mm\:ss\.ff} ({1:0.00}s)", duration, duration.TotalSeconds) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.TaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.ErrorTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.RackLocalTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.NonDataLocalTaskCount.ToString() });
            FinishedJobsTable.Rows.Add(row);
        }

        foreach( Guid jobId in metrics.FailedJobs )
        {
            JobStatus job = client.JobServer.GetJobStatus(jobId);
            HtmlTableRow row = new HtmlTableRow();
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"job.aspx?id={0}\">{{{0}}}</a>", jobId) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.JobName ?? "(unnamed)" });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.StartTime.ToString(System.Globalization.DateTimeFormatInfo.InvariantInfo.UniversalSortableDateTimePattern) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.EndTime.ToString() });
            TimeSpan duration = job.EndTime - job.StartTime;
            row.Cells.Add(new HtmlTableCell() { InnerText = string.Format(@"{0:hh\:mm\:ss\.ff} ({1:0.00}s)", duration, duration.TotalSeconds) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.TaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.ErrorTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.RackLocalTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.NonDataLocalTaskCount.ToString() });
            FailedJobsTable.Rows.Add(row);
        }
    }

    private HtmlTableCell CreateProgressCell(float progress)
    {
        progress *= 100;
        HtmlTableCell cell = new HtmlTableCell();
        cell.InnerHtml = string.Format("<div class=\"progressBar\"><div class=\"progressBarValue\" style=\"width:{0}%\">&nbsp;</div></div> {1:0.0}%", progress.ToString("0.0", System.Globalization.CultureInfo.InvariantCulture), progress);
        return cell;
    }
}
