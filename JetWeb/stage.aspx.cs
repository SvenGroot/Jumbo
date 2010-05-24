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

public partial class stage : System.Web.UI.Page
{
    private const string _datePattern = "yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fff'Z'";

    protected void Page_Load(object sender, EventArgs e)
    {
        Guid jobId = new Guid(Request.QueryString["job"]);
        string stageId = Request.QueryString["stage"];

        JetClient client = new JetClient();
        JobStatus job = client.JobServer.GetJobStatus(jobId);
        StageStatus stage = (from s in job.Stages
                             where s.StageId == stageId
                             select s).Single();

        Title = string.Format("Job {0} ({1}) stage {2} - Jumbo Jet", job.JobName, job.JobId, stage.StageId);
        HeaderText.InnerText = string.Format("Job {0} ({1}) stage {2}", job.JobName, job.JobId, stage.StageId);

        HtmlTableRow row = new HtmlTableRow();
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
            TimeSpan duration = endTime == null ? DateTime.UtcNow - startTime.Value : endTime.Value - startTime.Value;
            row.Cells.Add(new HtmlTableCell() { InnerText = string.Format("{0} ({1}s)", duration, duration.TotalSeconds) });
        }

        row.Cells.Add(CreateProgressCell(stage.Progress));
        row.Cells.Add(new HtmlTableCell() { InnerText = stage.Tasks.Count.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = stage.RunningTaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = stage.PendingTaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = stage.FinishedTaskCount.ToString() });

        StagesTable.Rows.Add(row);

        foreach( TaskStatus task in stage.Tasks )
        {
            row = CreateTaskTableRow(job, task, false, true);
            TasksTable.Rows.Add(row);
        }
    }

    private HtmlTableCell CreateProgressCell(float progress)
    {
        progress *= 100;
        HtmlTableCell cell = new HtmlTableCell();
        cell.InnerHtml = string.Format("<div class=\"progressBar\"><div class=\"progressBarValue\" style=\"width:{0}%\">&nbsp;</div></div> {1:0.0}%", progress.ToString("0.0", System.Globalization.CultureInfo.InvariantCulture), progress);
        return cell;
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
                row.Cells.Add(new HtmlTableCell() { InnerText = string.Format("{0} ({1}s)", duration, duration.TotalSeconds) });
            }
            else
            {
                row.Cells.Add(new HtmlTableCell() { InnerText = "" });
                row.Cells.Add(new HtmlTableCell() { InnerText = "" });
            }
            if( includeProgress )
                row.Cells.Add(new HtmlTableCell() { InnerText = task.TaskProgress == null ? "0.0 %" : task.TaskProgress.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"logfile.aspx?taskServer={0}&amp;port={1}&amp;job={2}&amp;task={3}&amp;attempt={4}\">View</a>", task.TaskServer.HostName, task.TaskServer.Port, job.JobId, task.TaskId, task.Attempts) });
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
