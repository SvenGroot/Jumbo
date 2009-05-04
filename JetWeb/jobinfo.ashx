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
    private const string _datePattern = "yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fff'Z'";

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
                    job.ToXml().Save(writer);
                }
                xmlStream.WriteTo(stream);
            }
            

            foreach( TaskStatus task in job.Tasks )
            {
                if( task.State >= TaskState.Finished )
                {
                    ITaskServerClientProtocol taskServer = JetClient.CreateTaskServerClient(task.TaskServer);
                    string log = taskServer.GetTaskLogFileContents(jobId, task.TaskId, task.Attempts, Int32.MaxValue);
                    if( log != null )
                    {
                        stream.PutNextEntry(new ZipEntry(string.Format("{0}_{1}.log", task.TaskId, task.Attempts)));
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