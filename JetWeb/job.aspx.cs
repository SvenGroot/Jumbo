// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Tkl.Jumbo.Jet;
using System.Web.UI.HtmlControls;
using System.Globalization;

public partial class job : System.Web.UI.Page
{
    private const string _datePattern = "yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fff'Z'";

    protected void Page_Load(object sender, EventArgs e)
    {
        Guid jobId = new Guid(Request.QueryString["id"]);
        JetClient client = new JetClient();
        JobStatus job = client.JobServer.GetJobStatus(jobId);
        HeaderText.InnerText = string.Format("Job {0} ({1})", job.JobName, jobId);
        Title = string.Format("Job {0} ({1}) - Jumbo Jet", job.JobName, jobId);

        HtmlTableRow row = new HtmlTableRow() { ID = "CurrentJobRow" };
        row.Cells.Add(new HtmlTableCell() { InnerText = job.StartTime.ToString(_datePattern, System.Globalization.CultureInfo.InvariantCulture) });
        TimeSpan duration;
        if( job.IsFinished )
        {
            _downloadLink.HRef = "jobinfo.ashx?id=" + jobId.ToString();
            _downloadLink.Visible = true;

            row.Cells.Add(new HtmlTableCell() { InnerText = job.EndTime.ToString(_datePattern, System.Globalization.CultureInfo.InvariantCulture) });
            duration = job.EndTime - job.StartTime;
        }
        else
        {
            Response.AppendHeader("Refresh", "5");
            row.Cells.Add(new HtmlTableCell());
            duration = DateTime.UtcNow - job.StartTime;
        }
        row.Cells.Add(new HtmlTableCell() { InnerText = string.Format("{0} ({1}s)", duration, duration.TotalSeconds) });
        row.Cells.Add(CreateProgressCell(job.Progress));
        row.Cells.Add(new HtmlTableCell() { InnerText = job.TaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.RunningTaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.UnscheduledTaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.FinishedTaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.ErrorTaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.NonDataLocalTaskCount.ToString() });
        RunningJobsTable.Rows.Add(row);

        foreach( StageStatus stage in job.Stages )
        {
            row = new HtmlTableRow();
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"stage.aspx?job={0}&amp;stage={1}\">{1}</a>", job.JobId, Server.HtmlEncode(stage.StageId)) });
            DateTime? startTime = stage.StartTime;
            if( startTime == null )
                row.Cells.Add(new HtmlTableCell());
            else
                row.Cells.Add(new HtmlTableCell() { InnerText = startTime.Value.ToString(_datePattern, CultureInfo.InvariantCulture) });

            DateTime? endTime = stage.EndTime;
            if( endTime == null )
                row.Cells.Add(new HtmlTableCell());
            else
                row.Cells.Add(new HtmlTableCell() { InnerText = endTime.Value.ToString(_datePattern, CultureInfo.InvariantCulture) });

            if( startTime == null )
                row.Cells.Add(new HtmlTableCell());
            else
            {
                duration = endTime == null ? DateTime.UtcNow - startTime.Value : endTime.Value - startTime.Value;
                row.Cells.Add(new HtmlTableCell() { InnerText = string.Format("{0} ({1}s)", duration, duration.TotalSeconds) });
            }

            row.Cells.Add(CreateProgressCell(stage.Progress));
            row.Cells.Add(new HtmlTableCell() { InnerText = stage.Tasks.Count.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = stage.RunningTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = stage.PendingTaskCount.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = stage.FinishedTaskCount.ToString() });

            StagesTable.Rows.Add(row);
        }

        _allTasksLink.HRef = "alltasks.aspx?id=" + job.JobId.ToString();
    }

    private HtmlTableCell CreateProgressCell(float progress)
    {
        progress *= 100;
        HtmlTableCell cell = new HtmlTableCell();
        cell.InnerHtml = string.Format("<div class=\"progressBar\"><div class=\"progressBarValue\" style=\"width:{0}%\">&nbsp;</div></div> {1:0.0}%", progress.ToString("0.0", System.Globalization.CultureInfo.InvariantCulture), progress);
        return cell;
    }
}
