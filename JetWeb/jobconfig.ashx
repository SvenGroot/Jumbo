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

        bool archived = context.Request.QueryString["archived"] == "true";
        JetClient client = new JetClient();
        context.Response.Write(client.JobServer.GetJobConfigurationFile(jobId, archived));
    }
}