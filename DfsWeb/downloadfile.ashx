<%@ WebHandler Language="C#" Class="downloadfile" %>

using System;
using System.Web;
using System.IO;
using Tkl.Jumbo.Dfs.FileSystem;

public class downloadfile : IHttpHandler {
    
    public void ProcessRequest (HttpContext context) {
        context.Response.ContentType = "application/octet-stream";
        string path = context.Request.QueryString["path"];
        context.Response.BufferOutput = false;
        FileSystemClient client = FileSystemClient.Create();
        context.Response.AppendHeader("Content-Disposition", "attachment; filename=" + client.Path.GetFileName(path));
        using( Stream stream = client.OpenFile(path) )
        {
            context.Response.AppendHeader("Content-Length", stream.Length.ToString());
            stream.CopyTo(context.Response.OutputStream);
        }
    }
 
    public bool IsReusable {
        get {
            return true;
        }
    }

}