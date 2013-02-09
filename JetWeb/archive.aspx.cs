using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Ookii.Jumbo.Jet;
using System.Web.UI.HtmlControls;

public partial class archive : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {
        JetClient client = new JetClient();

        ArchivedJob[] jobs = client.JobServer.GetArchivedJobs();

        foreach( ArchivedJob job in jobs.Reverse() )
        {
            HtmlTableRow row = new HtmlTableRow();
            row.Cells.Add(new HtmlTableCell() { InnerHtml = string.Format("<a href=\"job.aspx?id={0}&amp;archived=true\">{{{0}}}</a>", job.JobId) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.JobName ?? "(unnamed)" });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.IsSuccessful ? "Succeeded" : "Failed" });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.StartTime.ToString(System.Globalization.DateTimeFormatInfo.InvariantInfo.UniversalSortableDateTimePattern) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.EndTime.ToString(System.Globalization.DateTimeFormatInfo.InvariantInfo.UniversalSortableDateTimePattern) });
            TimeSpan duration = job.EndTime - job.StartTime;
            row.Cells.Add(new HtmlTableCell() { InnerText = string.Format("{0} ({1}s)", duration, duration.TotalSeconds) });
            row.Cells.Add(new HtmlTableCell() { InnerText = job.TaskCount.ToString() });
            ArchivedJobsTable.Rows.Add(row);
        }
    }
}