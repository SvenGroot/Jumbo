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
        if( job == null )
        {
            HeaderText.InnerText = "Job not found.";
            StageSummary.Visible = false;
        }
        else
        {
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

            // See if any of the tasks reports complex progress. If it does, all tasks that report complex progress should report the same additional progress values as that one.
            TaskProgress complexProgress = (from task in stage.Tasks
                                            where task.TaskProgress != null && task.TaskProgress.AdditionalProgressValues != null
                                            select task.TaskProgress).FirstOrDefault();

            int additionalProgressCount = 0;
            if( complexProgress != null )
            {
                foreach( HtmlTableCell cell in TasksTable.Rows[0].Cells )
                {
                    if( cell.InnerText != "Progress" )
                        cell.RowSpan = 2;
                    else
                        cell.ColSpan = complexProgress.AdditionalProgressValues.Count + 2;
                }

                HtmlTableRow progressHeaderRow = new HtmlTableRow();
                progressHeaderRow.Cells.Add(new HtmlTableCell("th") { InnerText = "Overall" });
                progressHeaderRow.Cells.Add(new HtmlTableCell("th") { InnerText = "Base" });
                foreach( AdditionalProgressValue value in complexProgress.AdditionalProgressValues )
                    progressHeaderRow.Cells.Add(new HtmlTableCell("th") { InnerText = job.GetFriendlyNameForAdditionalProgressCounter(value.SourceName) });
                TasksTable.Rows.Add(progressHeaderRow);
                additionalProgressCount = complexProgress.AdditionalProgressValues.Count;
            }

            foreach( TaskStatus task in stage.Tasks )
            {
                row = CreateTaskTableRow(job, task, false, additionalProgressCount);
                TasksTable.Rows.Add(row);
            }
        }
    }

    private HtmlTableCell CreateProgressCell(float progress)
    {
        progress *= 100;
        HtmlTableCell cell = new HtmlTableCell();
        cell.InnerHtml = string.Format("<div class=\"progressBar\"><div class=\"progressBarValue\" style=\"width:{0}%\">&nbsp;</div></div> {1:0.0}%", progress.ToString("0.0", System.Globalization.CultureInfo.InvariantCulture), progress);
        return cell;
    }

    private static HtmlTableRow CreateTaskTableRow(JobStatus job, TaskStatus task, bool useErrorEndTime, int additionalProgressCount)
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
            if( additionalProgressCount > 0 )
            {
                // No progress at all yet.
                if( task.TaskProgress == null )
                {
                    row.Cells.Add(new HtmlTableCell() { InnerText = task.Progress.ToString("P1") }); // Overall
                    row.Cells.Add(new HtmlTableCell() { InnerText = task.Progress.ToString("P1") }); // Base
                }
                else
                {
                    row.Cells.Add(new HtmlTableCell() { InnerText = task.TaskProgress.OverallProgress.ToString("P1") }); // Overall
                    row.Cells.Add(new HtmlTableCell() { InnerText = task.TaskProgress.Progress.ToString("P1") }); // Base
                }
                // Some tasks will not have reported complex progress yet. For those we set all remaining values to zero.
                if( task.TaskProgress == null || task.TaskProgress.AdditionalProgressValues == null )
                {
                    // Set to one if task is finished but has no complex progress
                    string value = ((task.TaskProgress == null || task.TaskProgress.OverallProgress < 1.0f) ? 0.0f : 1.0f).ToString("P1");
                    for( int x = 0; x < additionalProgressCount; ++x )
                        row.Cells.Add(new HtmlTableCell() { InnerText = value });
                }
                else
                {
                    foreach( AdditionalProgressValue value in task.TaskProgress.AdditionalProgressValues )
                        row.Cells.Add(new HtmlTableCell() { InnerText = value.Progress.ToString("P1") });
                }
            }
            else
                row.Cells.Add(new HtmlTableCell() { InnerText = task.Progress.ToString("P1") });
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"logfile.aspx?taskServer={0}&amp;port={1}&amp;job={2}&amp;task={3}&amp;attempt={4}\">View</a>", task.TaskServer.HostName, task.TaskServer.Port, job.JobId, task.TaskId, task.Attempts) });
        }
        else
        {
            row.Cells.Add(new HtmlTableCell() { InnerText = "" });
            row.Cells.Add(new HtmlTableCell() { InnerText = "" });
            row.Cells.Add(new HtmlTableCell() { InnerText = "" });
            row.Cells.Add(new HtmlTableCell() { InnerText = "" });
            if( additionalProgressCount > 0 )
            {
                for( int x = 0; x <= additionalProgressCount; ++x )
                    row.Cells.Add(new HtmlTableCell() { InnerText = "" });
            }
            row.Cells.Add(new HtmlTableCell() { InnerText = "" });
        }
        return row;
    }
}
