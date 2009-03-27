using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Tkl.Jumbo.Jet;
using System.Web.UI.HtmlControls;

public partial class job : System.Web.UI.Page
{
    private const string _datePattern = "yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fff'Z'";

    protected void Page_Load(object sender, EventArgs e)
    {
        Guid jobId = new Guid(Request.QueryString["id"]);
        HeaderText.InnerText = string.Format("Job {0}", jobId);
        Title = string.Format("Job {0} - Jumbo Jet", jobId);
        JetClient client = new JetClient();
        JobStatus job = client.JobServer.GetJobStatus(jobId);

        HtmlTableRow row = new HtmlTableRow() { ID = "CurrentJobRow" };
        row.Cells.Add(new HtmlTableCell() { InnerText = job.JobId.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.StartTime.ToString(_datePattern, System.Globalization.CultureInfo.InvariantCulture) });
        if( job.IsFinished )
        {
            _downloadLink.HRef = "jobinfo.ashx?id=" + jobId.ToString();
            _downloadLink.Visible = true;

            row.Cells.Add(new HtmlTableCell() { InnerText = job.EndTime.ToString(_datePattern, System.Globalization.CultureInfo.InvariantCulture) });
            TimeSpan duration = job.EndTime - job.StartTime;
            row.Cells.Add(new HtmlTableCell() { InnerText = string.Format("{0} ({1}s)", duration, duration.TotalSeconds) });
        }
        else
        {
            row.Cells.Add(new HtmlTableCell());
            row.Cells.Add(new HtmlTableCell());
        }
        row.Cells.Add(new HtmlTableCell() { InnerText = job.TaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.RunningTaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.UnscheduledTaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.FinishedTaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.ErrorTaskCount.ToString() });
        row.Cells.Add(new HtmlTableCell() { InnerText = job.NonDataLocalTaskCount.ToString() });
        RunningJobsTable.Rows.Add(row);

        foreach( TaskStatus task in job.Tasks )
        {
            row = new HtmlTableRow() { ID = "TaskStatusRow_" + task.TaskID };
            row.Cells.Add(new HtmlTableCell() { InnerText = task.TaskID });
            row.Cells.Add(new HtmlTableCell() { InnerText = task.State.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = task.TaskServer == null ? "" : task.TaskServer.ToString() });
            row.Cells.Add(new HtmlTableCell() { InnerText = task.Attempts.ToString() });
            if( task.State >= TaskState.Running && task.TaskServer != null )
            {
                row.Cells.Add(new HtmlTableCell() { InnerText = task.StartTime.ToString(_datePattern, System.Globalization.CultureInfo.InvariantCulture) });
                if( task.State == TaskState.Finished )
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
                row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"logfile.aspx?taskServer={0}&amp;port={1}&amp;job={2}&amp;task={3}&amp;attempt={4}\">View</a>", task.TaskServer.HostName, task.TaskServer.Port, job.JobId, task.TaskID, task.Attempts) });
            }
            else
            {
                row.Cells.Add(new HtmlTableCell() { InnerText = "" });
                row.Cells.Add(new HtmlTableCell() { InnerText = "" });
                row.Cells.Add(new HtmlTableCell() { InnerText = "" });
                row.Cells.Add(new HtmlTableCell() { InnerText = "" });
            }
            TasksTable.Rows.Add(row);
        }
    }
}
