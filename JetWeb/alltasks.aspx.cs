// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Ookii.Jumbo.Jet;
using System.Web.UI.HtmlControls;

public partial class alltasks : System.Web.UI.Page
{
    private const string _datePattern = "yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fff'Z'";

    protected void Page_Load(object sender, EventArgs e)
    {
        Guid jobId = new Guid(Request.QueryString["id"]);
        HeaderText.InnerText = string.Format("Job {0}", jobId);
        Title = string.Format("Job {0} - Jumbo Jet", jobId);
        JetClient client = new JetClient();
        JobStatus job;
        if( Request.QueryString["archived"] == "true" )
            job = client.JobServer.GetArchivedJobStatus(jobId);
        else
            job = client.JobServer.GetJobStatus(jobId);

        HtmlTableRow row = new HtmlTableRow() { ID = "CurrentJobRow" };
        row.Cells.Add(new HtmlTableCell() { InnerText = job.JobId.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.StartTime.ToString(_datePattern, System.Globalization.CultureInfo.InvariantCulture) });
        TimeSpan duration;
        if( job.IsFinished )
        {
            row.Cells.Add(new HtmlTableCell() { InnerText = job.EndTime.ToString(_datePattern, System.Globalization.CultureInfo.InvariantCulture) });
            duration = job.EndTime - job.StartTime;
        }
        else
        {
            row.Cells.Add(new HtmlTableCell());
            duration = DateTime.UtcNow - job.StartTime;
        }
        row.Cells.Add(new HtmlTableCell() { InnerText = string.Format(@"{0:hh\:mm\:ss\.ff} ({1:0.00}s)", duration, duration.TotalSeconds) });
        row.Cells.Add(new HtmlTableCell() { InnerText = (job.Progress * 100).ToString("0.0'%'") });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.TaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.RunningTaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.UnscheduledTaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.FinishedTaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.ErrorTaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.RackLocalTaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.NonDataLocalTaskCount.ToString() });
        RunningJobsTable.Rows.Add(row);

        foreach( StageStatus stage in job.Stages )
        {
            foreach( TaskStatus task in stage.Tasks )
            {
                row = CreateTaskTableRow(job, task, false, true);
                TasksTable.Rows.Add(row);
            }
        }

        if( job.ErrorTaskCount > 0 )
        {
            _failedTaskAttemptsPlaceHolder.Visible = true;
            foreach( TaskStatus task in job.FailedTaskAttempts )
            {
                row = CreateTaskTableRow(job, task, true, false);
                _failedTaskAttemptsTable.Rows.Add(row);
            }
        }
    }

    private static HtmlTableRow CreateTaskTableRow(JobStatus job, TaskStatus task, bool useErrorEndTime, bool includeProgress)
    {
        HtmlTableRow row = new HtmlTableRow() { ID = "TaskStatusRow_" + task.TaskId };
        row.Cells.Add(new HtmlTableCell() { InnerText = task.TaskId });
        row.Cells.Add(new HtmlTableCell() { InnerText = task.State.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = task.TaskServer == null ? "" : task.TaskServer.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = task.Attempts.ToString() });
        if( task.State >= TaskState.Running && task.TaskServer != null )
        {
            row.Cells.Add(new HtmlTableCell() { InnerText = task.StartTime.ToString(_datePattern, System.Globalization.CultureInfo.InvariantCulture) });
            if( task.State == TaskState.Finished || (useErrorEndTime && task.State == TaskState.Error) )
            {
                row.Cells.Add(new HtmlTableCell() { InnerText = task.EndTime.ToString(_datePattern, System.Globalization.CultureInfo.InvariantCulture) });
                TimeSpan duration = task.EndTime - task.StartTime;
                row.Cells.Add(new HtmlTableCell() { InnerText = string.Format(@"{0:hh\:mm\:ss\.ff} ({1:0.00}s)", duration, duration.TotalSeconds) });
            }
            else
            {
                row.Cells.Add(new HtmlTableCell() { InnerText = "" });
                row.Cells.Add(new HtmlTableCell() { InnerText = "" });
            }
            if( includeProgress )
                row.Cells.Add(new HtmlTableCell() { InnerText = task.Progress.ToString("P1") }); // This page does not display complex progress.
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"logfile.aspx?taskServer={0}&amp;port={1}&amp;job={2}&amp;task={3}&amp;attempt={4}&amp;maxSize=100KB\">Last 100KB</a>, <a href=\"logfile.aspx?taskServer={0}&amp;port={1}&amp;job={2}&amp;task={3}&amp;attempt={4}&amp;maxSize=0\">all</a>", task.TaskServer.HostName, task.TaskServer.Port, job.JobId, task.TaskId, task.Attempts) });
        }
        else
        {
            row.Cells.Add(new HtmlTableCell() { InnerText = "" });
            row.Cells.Add(new HtmlTableCell() { InnerText = "" });
            row.Cells.Add(new HtmlTableCell() { InnerText = "" });
            row.Cells.Add(new HtmlTableCell() { InnerText = "" });
            row.Cells.Add(new HtmlTableCell() { InnerText = "" });
        }
        return row;
    }
}
