<%@ WebHandler Language="C#" Class="jobinfo" %>

using System;
using System.Web;
using System.Xml;
using System.IO;
using System.Linq;
using System.Threading;
using ICSharpCode.SharpZipLib.Zip;
using Tkl.Jumbo;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Dfs;

public class jobinfo : IHttpHandler
{
    private sealed class AsynchronousLogFileDownload : IDisposable
    {
        private readonly ManualResetEvent _completedEvent = new ManualResetEvent(false);
        private readonly ZipOutputStream _zipStream;
        private readonly Guid _jobId;
        private readonly ServerAddress _server;
        private bool _disposed;

        public AsynchronousLogFileDownload(ServerAddress server, ZipOutputStream zipStream, Guid jobId)
        {
            _server = server;
            _zipStream = zipStream;
            _jobId = jobId;
            
            Run();
        }
        
        public ManualResetEvent CompletedEvent
        {
            get { return _completedEvent; }
        }

        public void Run()
        {
            ThreadPool.QueueUserWorkItem(DownloadLogFiles);
        }

        private void DownloadLogFiles(object state)
        {
            ITaskServerClientProtocol taskServer = JetClient.CreateTaskServerClient(_server);
            byte[] logBytes = taskServer.GetCompressedTaskLogFiles(_jobId);
            lock( _zipStream )
            {
                _zipStream.PutNextEntry(new ZipEntry(string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}-{1}.zip", _server.HostName, _server.Port)));
                _zipStream.Write(logBytes, 0, logBytes.Length);
            }

            CompletedEvent.Set();
        }

        #region IDisposable Members

        public void Dispose()
        {
            if( !_disposed )
            {
                _disposed = true;
                ((IDisposable)_completedEvent).Dispose();
            }
            GC.SuppressFinalize(this);
        }

        #endregion
    }
    
    public void ProcessRequest(HttpContext context)
    {
        Guid jobId = new Guid(context.Request.QueryString["id"]);
        JetClient client = new JetClient();
        DfsClient dfsClient = new DfsClient();
        JobStatus job = client.JobServer.GetJobStatus(jobId);
        context.Response.ContentType = "application/zip";
        string fileName = string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0},%20{1}%20({2}).zip", typeof(DfsClient).Assembly.GetName().Version.Revision, job.JobName, job.JobId);
        context.Response.AddHeader("Content-Disposition", "attachment; filename=" + fileName);
        context.Response.Cache.SetCacheability(HttpCacheability.NoCache);
        using( ZipOutputStream stream = new ZipOutputStream(context.Response.OutputStream) )
        {
            stream.SetLevel(9);

            stream.PutNextEntry(new ZipEntry("config.xml"));
            string configFilePath = DfsPath.Combine(DfsPath.Combine(client.Configuration.JobServer.JetDfsPath, "job_" + job.JobId.ToString("B")), Job.JobConfigFileName);
            using( DfsInputStream configStream = dfsClient.OpenFile(configFilePath) )
            {
                configStream.CopyTo(stream);
            }
            
            stream.PutNextEntry(new ZipEntry("summary.xml"));
            using( MemoryStream xmlStream = new MemoryStream() )
            {
                using( XmlWriter writer = XmlWriter.Create(xmlStream) )
                {
                    job.ToXml().Save(writer);
                }
                xmlStream.WriteTo(stream);
            }

            var servers = (from stage in job.Stages 
                           from task in stage.Tasks
                           select task.TaskServer).Distinct();

            var downloads = (from server in servers
                             select new AsynchronousLogFileDownload(server, stream, jobId)).ToList();

            foreach( var download in downloads )
            {
                download.CompletedEvent.WaitOne();
                download.Dispose();
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