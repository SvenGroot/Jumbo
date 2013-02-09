<%@ WebHandler Language="C#" Class="jobconfig" %>

using System;
using System.Web;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.Jet;

public class jobconfig : IHttpHandler
{
    public bool IsReusable
    {
        get { return true; }
    }

    public void ProcessRequest(HttpContext context)
    {
        Guid jobId = new Guid(context.Request.QueryString["id"]);

        context.Response.ContentType = "text/xml";
        context.Response.Charset = "utf-8";

        if( context.Request.QueryString["archived"] == "true" )
        {
            JetClient client = new JetClient();
            context.Response.Write(client.JobServer.GetArchivedJobConfiguration(jobId));
        }
        else
        {
            FileSystemClient fileSystemClient = FileSystemClient.Create();
            string configFilePath = fileSystemClient.Path.Combine(fileSystemClient.Path.Combine(JetConfiguration.GetConfiguration().JobServer.JetDfsPath, "job_" + jobId.ToString("B")), Job.JobConfigFileName);
            using( System.IO.Stream configStream = fileSystemClient.OpenFile(configFilePath) )
            {
                configStream.CopyTo(context.Response.OutputStream);
            }
        }
    }
}