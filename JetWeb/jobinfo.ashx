<%@ WebHandler Language="C#" Class="jobinfo" %>

using System;
using System.Web;
using System.Xml;
using System.IO;
using ICSharpCode.SharpZipLib.Zip;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Dfs;

public class jobinfo : IHttpHandler
{

    public void ProcessRequest(HttpContext context)
    {
        Guid jobId = new Guid(context.Request.QueryString["id"]);
        JetClient client = new JetClient();
        JobStatus job = client.JobServer.GetJobStatus(jobId);
        context.Response.ContentType = "application/zip";
        context.Response.AddHeader("Content-Disposition", "attachment; filename=job_" + jobId.ToString() + ".zip");
        context.Response.Cache.SetCacheability(HttpCacheability.NoCache);
        using( ZipOutputStream stream = new ZipOutputStream(context.Response.OutputStream) )
        {
            stream.SetLevel(9);

            stream.PutNextEntry(new ZipEntry("job.xml"));
            using( MemoryStream xmlStream = new MemoryStream() )
            {
                using( XmlWriter writer = XmlWriter.Create(xmlStream) )
                {
                    writer.WriteStartDocument();

                    writer.WriteStartElement("Job");
                    writer.WriteAttributeString("id", jobId.ToString());

                    writer.WriteStartElement("JobInfo");
                    writer.WriteAttributeString("startTime", job.StartTime.ToString(System.Globalization.DateTimeFormatInfo.InvariantInfo.UniversalSortableDateTimePattern));
                    writer.WriteAttributeString("endTime", job.EndTime.ToString(System.Globalization.DateTimeFormatInfo.InvariantInfo.UniversalSortableDateTimePattern));
                    writer.WriteAttributeString("duration", (job.EndTime - job.StartTime).TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture));
                    writer.WriteAttributeString("tasks", job.TaskCount.ToString(System.Globalization.CultureInfo.InvariantCulture));
                    writer.WriteAttributeString("finishedTasks", job.FinishedTaskCount.ToString(System.Globalization.CultureInfo.InvariantCulture));
                    writer.WriteAttributeString("errors", job.ErrorTaskCount.ToString(System.Globalization.CultureInfo.InvariantCulture));
                    writer.WriteAttributeString("nonDataLocalTasks", job.NonDataLocalTaskCount.ToString(System.Globalization.CultureInfo.InvariantCulture));
                    writer.WriteEndElement(); // JobInfo

                    writer.WriteStartElement("Tasks");

                    foreach( TaskStatus task in job.Tasks )
                    {
                        writer.WriteStartElement("Task");
                        writer.WriteAttributeString("id", task.TaskID);
                        writer.WriteAttributeString("state", task.State.ToString(System.Globalization.CultureInfo.InvariantCulture));
                        writer.WriteAttributeString("server", task.TaskServer.ToString());
                        writer.WriteAttributeString("attempts", task.Attempts.ToString(System.Globalization.CultureInfo.InvariantCulture));
                        writer.WriteAttributeString("startTime", task.StartTime.ToString(System.Globalization.DateTimeFormatInfo.InvariantInfo.UniversalSortableDateTimePattern));
                        writer.WriteAttributeString("endTime", task.EndTime.ToString(System.Globalization.DateTimeFormatInfo.InvariantInfo.UniversalSortableDateTimePattern));
                        writer.WriteAttributeString("duration", (task.EndTime - task.StartTime).TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture));
                        writer.WriteEndElement(); // Task
                    }

                    writer.WriteEndElement(); // Tasks

                    writer.WriteEndElement(); // Job

                    writer.WriteEndDocument();
                }
                xmlStream.WriteTo(stream);
            }
            

            foreach( TaskStatus task in job.Tasks )
            {
                if( task.State >= TaskState.Finished )
                {
                    ITaskServerClientProtocol taskServer = JetClient.CreateTaskServerClient(task.TaskServer);
                    string log = taskServer.GetTaskLogFileContents(jobId, task.TaskID, task.Attempts, Int32.MaxValue);
                    if( log != null )
                    {
                        stream.PutNextEntry(new ZipEntry(string.Format("{0}_{1}.log", task.TaskID, task.Attempts)));
                        byte[] logBytes = System.Text.Encoding.UTF8.GetBytes(log);
                        stream.Write(logBytes, 0, logBytes.Length);
                    }
                }
            }
        }
    }

    public bool IsReusable
    {
        get
        {
            return true;
        }
    }

}