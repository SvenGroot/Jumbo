<%@ WebHandler Language="C#" Class="jobconfig" %>

using System;
using System.Web;
using Tkl.Jumbo;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet;

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
            DfsClient dfsClient = new DfsClient();
            string configFilePath = DfsPath.Combine(DfsPath.Combine(JetConfiguration.GetConfiguration().JobServer.JetDfsPath, "job_" + jobId.ToString("B")), Job.JobConfigFileName);
            using( DfsInputStream configStream = dfsClient.OpenFile(configFilePath) )
            {
                configStream.CopyTo(context.Response.OutputStream);
            }
        }
    }
}