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
using System.Text;

public partial class job : System.Web.UI.Page
{
    private const string _datePattern = "yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fff'Z'";

    protected void Page_Load(object sender, EventArgs e)
    {
        Guid jobId = new Guid(Request.QueryString["id"]);
        JetClient client = new JetClient();
        JobStatus job;
        bool archived = Request.QueryString["archived"] == "true";
        if( archived )
            job = client.JobServer.GetArchivedJobStatus(jobId);
        else
            job = client.JobServer.GetJobStatus(jobId);
        if( job == null )
        {
            HeaderText.InnerText = "Job not found.";
            JobSummary.Visible = false;
        }
        else
        {
            HeaderText.InnerText = string.Format("Job {0} ({1})", job.JobName, jobId);
            Title = string.Format("Job {0} ({1}) - Jumbo Jet", job.JobName, jobId);

            HtmlTableRow row = new HtmlTableRow() { ID = "CurrentJobRow" };
            row.Cells.Add(new HtmlTableCell() { InnerText = job.StartTime.ToString(_datePattern, System.Globalization.CultureInfo.InvariantCulture) });
            TimeSpan duration;
            _configLink.HRef = "jobconfig.ashx?id=" + jobId.ToString();
            if( archived )
                _configLink.HRef += "&amp;archived=true";
            if( job.IsFinished )
            {
                _downloadLink.HRef = "jobinfo.ashx?id=" + jobId.ToString();
                if( archived )
                    _downloadLink.HRef += "&amp;archived=true";
                _downloadLink.Visible = true;

                row.Cells.Add(new HtmlTableCell() { InnerText = job.EndTime.ToString(_datePattern, System.Globalization.CultureInfo.InvariantCulture) });
                duration = job.EndTime - job.StartTime;
            }
            else
            {
                int refresh;
                if( !int.TryParse(Request.QueryString["refresh"], NumberStyles.Integer, CultureInfo.InvariantCulture, out refresh) || refresh <= 0 )
                    refresh = 5;
                Response.AppendHeader("Refresh", refresh.ToString(CultureInfo.InvariantCulture));
                row.Cells.Add(new HtmlTableCell());
                duration = DateTime.UtcNow - job.StartTime;
            }
            row.Cells.Add(new HtmlTableCell() { InnerText = string.Format(@"{0:hh\:mm\:ss\.ff} ({1:0.00}s)", duration, duration.TotalSeconds) });
            row.Cells.Add(new HtmlTableCell() { InnerHtml = CreateProgressBar(job.Progress) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.TaskCount.ToString() });
            CreateTasksLinkCell(row, jobId, null, TaskState.Running, job.RunningTaskCount.ToString(), archived);
            CreateTasksLinkCell(row, jobId, null, TaskState.Created, job.UnscheduledTaskCount.ToString(), archived);
            CreateTasksLinkCell(row, jobId, null, TaskState.Finished, job.FinishedTaskCount.ToString(), archived);
            CreateTasksLinkCell(row, jobId, null, TaskState.Error, job.ErrorTaskCount.ToString(), archived);
            CreateTasksLinkCell(row, jobId, 1, job.RackLocalTaskCount.ToString(), archived);
            CreateTasksLinkCell(row, jobId, 2, job.NonDataLocalTaskCount.ToString(), archived);
            RunningJobsTable.Rows.Add(row);

            if( job.IsFinished && !job.IsSuccessful )
            {
                _failureReason.Visible = true;
                _failureReason.InnerText = "Job failed: " + (job.FailureReason ?? "Unknown reason.");
            }


            foreach( StageStatus stage in job.Stages )
            {
                row = new HtmlTableRow();
                row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"tasks.aspx?job={0}&amp;stage={1}{2}\">{1}</a>", job.JobId, Server.HtmlEncode(stage.StageId), archived ? "&amp;archived=true" : "") });
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
                    row.Cells.Add(new HtmlTableCell() { InnerText = string.Format(@"{0:hh\:mm\:ss\.ff} ({1:0.00}s)", duration, duration.TotalSeconds) });
                }

                row.Cells.Add(CreateProgressCell(job, stage, true));
                row.Cells.Add(new HtmlTableCell() { InnerText = stage.Tasks.Count.ToString() });
                CreateTasksLinkCell(row, jobId, stage.StageId, TaskState.Running, stage.RunningTaskCount.ToString(), archived);
                CreateTasksLinkCell(row, jobId, stage.StageId, TaskState.Created, stage.PendingTaskCount.ToString(), archived);
                CreateTasksLinkCell(row, jobId, stage.StageId, TaskState.Finished, stage.FinishedTaskCount.ToString(), archived);

                StagesTable.Rows.Add(row);
            }

            _allTasksLink.HRef = "alltasks.aspx?id=" + job.JobId.ToString();
            if( archived )
                _allTasksLink.HRef += "&amp;archived=true";

            CreateMetricsTable(job);
        }
    }

    private HtmlTableCell CreateProgressCell(JobStatus job, StageStatus stage, bool complexProgress)
    {
        float progress;
        TaskProgress stageProgress = null;
        if( complexProgress )
        {
            stageProgress = stage.StageProgress;
            progress = stageProgress.OverallProgress;
        }
        else
            progress = stage.Progress;
        HtmlTableCell cell = new HtmlTableCell();
        if( complexProgress && stageProgress.AdditionalProgressValues != null )
        {
            StringBuilder builder = new StringBuilder(400);
            builder.Append("<div>");
            builder.Append(CreateProgressBar(stageProgress.OverallProgress));
            builder.Append("</div>");
            builder.Append("<div class=\"additionalProgress\">Base: ");
            builder.Append(CreateProgressBar(stageProgress.Progress));
            builder.Append("</div>");
            foreach( AdditionalProgressValue value in stageProgress.AdditionalProgressValues )
            {
                builder.Append("<div class=\"additionalProgress\">");
                builder.Append(Server.HtmlEncode(job.GetFriendlyNameForAdditionalProgressCounter(value.SourceName)));
                builder.Append(": ");
                builder.Append(CreateProgressBar(value.Progress));
                builder.Append("</div>");
            }

            cell.InnerHtml = builder.ToString();
        }
        else
            cell.InnerHtml = CreateProgressBar(progress);
        return cell;
    }

    private static string CreateProgressBar(float progress)
    {
        return string.Format("<div class=\"progressBar\"><div class=\"progressBarValue\" style=\"width:{0}%\">&nbsp;</div></div> {1:P1}", (progress * 100).ToString("0.0", System.Globalization.CultureInfo.InvariantCulture), progress);
    }

    private static void CreateTasksLinkCell(HtmlTableRow row, Guid jobId, string stageId, TaskState state, string text, bool archived)
    {
        if( stageId == null )
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"tasks.aspx?job={0}&amp;state={1}{2}\">{3}</a>", jobId, state, archived ? "&amp;archived=true" : "", HttpUtility.HtmlEncode(text)) });
        else
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"tasks.aspx?job={0}&amp;stage={1}&amp;state={2}{3}\">{4}</a>", jobId, HttpUtility.UrlEncode(stageId), state, archived ? "&amp;archived=true" : "", HttpUtility.HtmlEncode(text)) });
    }

    private static void CreateTasksLinkCell(HtmlTableRow row, Guid jobId, int dataDistance, string text, bool archived)
    {
        row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format(CultureInfo.InvariantCulture, "<a href=\"tasks.aspx?job={0}&amp;dataDistance={1}{2}\">{3}</a>", jobId, dataDistance, archived ? "&amp;archived=true" : "", HttpUtility.HtmlEncode(text)) });
    }

    private void CreateMetricsTable(JobStatus job)
    {
        foreach( StageStatus stage in job.Stages )
        {
            TaskMetrics metrics = stage.Metrics;
            HtmlTableCell headerCell = new HtmlTableCell("th") { InnerText = stage.StageId };
            headerCell.Attributes.Add("scope", "col");
            MetricsTable.Rows[0].Cells.Add(headerCell);
            MetricsTable.Rows[1].Cells.Add(new HtmlTableCell() { InnerText = metrics.InputRecords.ToString("#,0", CultureInfo.InvariantCulture) });
            MetricsTable.Rows[2].Cells.Add(new HtmlTableCell() { InnerText = metrics.InputBytes.ToString("#,0", CultureInfo.InvariantCulture) });
            MetricsTable.Rows[3].Cells.Add(new HtmlTableCell() { InnerText = metrics.OutputRecords.ToString("#,0", CultureInfo.InvariantCulture) });
            MetricsTable.Rows[4].Cells.Add(new HtmlTableCell() { InnerText = metrics.OutputBytes.ToString("#,0", CultureInfo.InvariantCulture) });
            MetricsTable.Rows[5].Cells.Add(new HtmlTableCell() { InnerText = metrics.DfsBytesRead.ToString("#,0", CultureInfo.InvariantCulture) });
            MetricsTable.Rows[6].Cells.Add(new HtmlTableCell() { InnerText = metrics.DfsBytesWritten.ToString("#,0", CultureInfo.InvariantCulture) });
            MetricsTable.Rows[7].Cells.Add(new HtmlTableCell() { InnerText = metrics.LocalBytesRead.ToString("#,0", CultureInfo.InvariantCulture) });
            MetricsTable.Rows[8].Cells.Add(new HtmlTableCell() { InnerText = metrics.LocalBytesWritten.ToString("#,0", CultureInfo.InvariantCulture) });
            MetricsTable.Rows[9].Cells.Add(new HtmlTableCell() { InnerText = metrics.NetworkBytesRead.ToString("#,0", CultureInfo.InvariantCulture) });
            MetricsTable.Rows[10].Cells.Add(new HtmlTableCell() { InnerText = metrics.NetworkBytesWritten.ToString("#,0", CultureInfo.InvariantCulture) });
            MetricsTable.Rows[11].Cells.Add(new HtmlTableCell() { InnerText = metrics.DynamicallyAssignedPartitions.ToString("#,0", CultureInfo.InvariantCulture) });
            MetricsTable.Rows[12].Cells.Add(new HtmlTableCell() { InnerText = metrics.DiscardedPartitions.ToString("#,0", CultureInfo.InvariantCulture) });
        }
    }
}
