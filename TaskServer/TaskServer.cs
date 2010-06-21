// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo;
using System.Net;
using System.Threading;
using Tkl.Jumbo.Dfs;
using System.Diagnostics;
using System.IO;

namespace TaskServerApplication
{
    public class TaskServer : ITaskServerUmbilicalProtocol, ITaskServerClientProtocol
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskServer));

        private readonly int _heartbeatInterval = 3000;

        private IJobServerHeartbeatProtocol _jobServer;
        private volatile bool _running;
        private readonly List<JetHeartbeatData> _pendingHeartbeatData = new List<JetHeartbeatData>();
        private TaskRunner _taskRunner;
        private static object _startupLock = new object();
        private FileChannelServer _fileServer;
        private readonly Dictionary<Guid, Dictionary<string, long>> _uncompressedTemporaryFileSizes = new Dictionary<Guid, Dictionary<string, long>>();

        private TaskServer()
            : this(JetConfiguration.GetConfiguration(), DfsConfiguration.GetConfiguration())
        {
        }

        private TaskServer(JetConfiguration config, DfsConfiguration dfsConfiguration)
        {
            if( config == null )
                throw new ArgumentNullException("config");

            Configuration = config;
            DfsConfiguration = dfsConfiguration;

            if( !System.IO.Directory.Exists(config.TaskServer.TaskDirectory) )
                System.IO.Directory.CreateDirectory(config.TaskServer.TaskDirectory);

            _jobServer = JetClient.CreateJobServerHeartbeatClient(config);
            _heartbeatInterval = config.TaskServer.HeartbeatInterval;
        }

        public static TaskServer Instance { get; private set; }

        public JetConfiguration Configuration { get; private set; }
        public DfsConfiguration DfsConfiguration { get; private set; }
        public ServerAddress LocalAddress { get; private set; }
        public bool IsRunning { get { return _running; } }

        public static void Run()
        {
            Run(JetConfiguration.GetConfiguration(), DfsConfiguration.GetConfiguration());
        }

        public static void Run(JetConfiguration jetConfig, DfsConfiguration dfsConfig)
        {
            // Prevent type references in job configurations from loading assemblies into the task server.
            TypeReference.ResolveTypes = false;

            lock( _startupLock )
            {
                Instance = new TaskServer(jetConfig, dfsConfig);

                RpcHelper.RegisterServerChannels(jetConfig.TaskServer.Port, jetConfig.TaskServer.ListenIPv4AndIPv6);
                RpcHelper.RegisterService(typeof(RpcServer), "TaskServer");
            }

            Instance.RunInternal();
        }

        public static void Shutdown()
        {
            lock( _startupLock )
            {
                Instance.ShutdownInternal();

                RpcHelper.UnregisterServerChannels(Instance.Configuration.TaskServer.Port);

                Instance = null;
            }
        }

        public void NotifyTaskStatusChanged(Guid jobID, TaskAttemptId taskAttemptId, TaskAttemptStatus newStatus, TaskProgress progress, TaskMetrics metrics)
        {
            AddDataForNextHeartbeat(new TaskStatusChangedJetHeartbeatData(jobID, taskAttemptId, newStatus, progress, metrics));
            if( newStatus == TaskAttemptStatus.Completed )
                SendHeartbeat();
        }

        public string GetJobDirectory(Guid jobID)
        {
            return System.IO.Path.Combine(Configuration.TaskServer.TaskDirectory, "job_" + jobID.ToString());
        }

        #region ITaskServerUmbilicalProtocol Members

        public void ReportCompletion(Guid jobID, TaskAttemptId taskAttemptId, TaskMetrics metrics)
        {
            if( taskAttemptId == null )
                throw new ArgumentNullException("taskID");
            string fullTaskID = Job.CreateFullTaskId(jobID, taskAttemptId);
            _log.DebugFormat("ReportCompletion, fullTaskID = \"{0}\"", fullTaskID);
            _taskRunner.ReportCompletion(fullTaskID, metrics);
        }

        public void ReportProgress(Guid jobId, TaskAttemptId taskAttemptId, TaskProgress progress)
        {
            _taskRunner.ReportProgress(Job.CreateFullTaskId(jobId, taskAttemptId), progress);
        }

        public void SetUncompressedTemporaryFileSize(Guid jobId, string fileName, long uncompressedSize)
        {
            _log.DebugFormat("Uncompressed file size of job {0} file {1} is {2}.", jobId, fileName, uncompressedSize);
            lock( _uncompressedTemporaryFileSizes )
            {
                Dictionary<string, long> jobFileSizes;
                if( !_uncompressedTemporaryFileSizes.TryGetValue(jobId, out jobFileSizes) )
                {
                    jobFileSizes = new Dictionary<string, long>();
                    _uncompressedTemporaryFileSizes.Add(jobId, jobFileSizes);
                }
                jobFileSizes[fileName] = uncompressedSize;
            }
        }

        public long GetUncompressedTemporaryFileSize(Guid jobId, string fileName)
        {
            lock( _uncompressedTemporaryFileSizes )
            {
                Dictionary<string, long> jobFileSizes;
                if( _uncompressedTemporaryFileSizes.TryGetValue(jobId, out jobFileSizes) )
                {
                    long uncompressedSize;
                    if( jobFileSizes.TryGetValue(fileName, out uncompressedSize) )
                        return uncompressedSize;
                }
                return -1;
            }
        }

        public void RegisterTcpChannelPort(Guid jobId, TaskAttemptId taskAttemptId, int port)
        {
            if( taskAttemptId == null )
                throw new ArgumentNullException("taskId");
            if( port <= 0 )
                throw new ArgumentOutOfRangeException("port", "Port must be greater than zero.");

            string fullTaskId = Job.CreateFullTaskId(jobId, taskAttemptId);
            _log.InfoFormat("Task {0} has is registering TCP channel port {1}.", fullTaskId, port);
            _taskRunner.RegisterTcpChannelPort(fullTaskId, port);
        }

        #endregion

        #region ITaskServerClientProtocol Members

        public int FileServerPort
        {
            get { return Configuration.TaskServer.FileServerPort; }
        }

        public TaskAttemptStatus GetTaskStatus(Guid jobId, TaskAttemptId taskAttemptId)
        {
            string fullTaskID = Job.CreateFullTaskId(jobId, taskAttemptId);
            _log.DebugFormat("GetTaskStatus, fullTaskID = \"{0}\"", fullTaskID);
            TaskAttemptStatus status = _taskRunner.GetTaskStatus(fullTaskID);
            _log.DebugFormat("Task {0} status is {1}.", fullTaskID, status);
            return status;
        }

        public string GetOutputFileDirectory(Guid jobId)
        {
            _log.DebugFormat("GetOutputFileDirectory, jobId = \"{0}\"", jobId);
            return GetJobDirectory(jobId);
        }

        public string GetLogFileContents(int maxSize)
        {
            _log.Debug("GetLogFileContents");
            foreach( log4net.Appender.IAppender appender in log4net.LogManager.GetRepository().GetAppenders() )
            {
                log4net.Appender.FileAppender fileAppender = appender as log4net.Appender.FileAppender;
                if( fileAppender != null )
                {
                    using( System.IO.FileStream stream = System.IO.File.Open(fileAppender.File, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite) )
                    using( System.IO.StreamReader reader = new System.IO.StreamReader(stream) )
                    {
                        if( stream.Length > maxSize )
                        {
                            stream.Position = stream.Length - maxSize;
                            reader.ReadLine(); // Scan to the first new line.
                        }
                        return reader.ReadToEnd();
                    }
                }
            }
            return null;
        }

        public string GetTaskLogFileContents(Guid jobId, TaskAttemptId taskAttemptId, int maxSize)
        {
            _log.DebugFormat("GetTaskLogFileContents; jobId = {{{0}}}, taskAttemptId = \"{1}\", maxSize = {2}", jobId, taskAttemptId, maxSize);
            string jobDirectory = GetJobDirectory(jobId);
            string logFileName = System.IO.Path.Combine(jobDirectory, taskAttemptId.ToString() + ".log");
            if( System.IO.File.Exists(logFileName) )
            {
                using( System.IO.FileStream stream = System.IO.File.Open(logFileName, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite) )
                using( System.IO.StreamReader reader = new System.IO.StreamReader(stream) )
                {
                    if( stream.Length > maxSize )
                    {
                        stream.Position = stream.Length - maxSize;
                        reader.ReadLine(); // Scan to the first new line.
                    }
                    return reader.ReadToEnd();
                }
            }
            return null;
        }

        public byte[] GetCompressedTaskLogFiles(Guid jobId)
        {
            _log.DebugFormat("GetCompressedTaskLogFiles; jobId = {{{0}}}", jobId);
            string jobDirectory = GetJobDirectory(jobId);
            if( System.IO.Directory.Exists(jobDirectory) )
            {
                string[] logFiles = System.IO.Directory.GetFiles(jobDirectory, "*.log");
                if( logFiles.Length > 0 )
                {
                    using( MemoryStream outputStream = new MemoryStream() )
                    using( ICSharpCode.SharpZipLib.Zip.ZipOutputStream zipStream = new ICSharpCode.SharpZipLib.Zip.ZipOutputStream(outputStream) )
                    {
                        zipStream.SetLevel(9);

                        foreach( string logFile in logFiles )
                        {
                            zipStream.PutNextEntry(new ICSharpCode.SharpZipLib.Zip.ZipEntry(System.IO.Path.GetFileName(logFile)));
                            using( System.IO.FileStream stream = System.IO.File.Open(logFile, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite) )
                            {
                                stream.CopyTo(zipStream);
                            }
                        }

                        zipStream.Finish();

                        return outputStream.ToArray();
                    }
                }
            }

            return null;
        }

        public string GetTaskProfileOutput(Guid jobId, TaskAttemptId taskAttemptId)
        {
            _log.DebugFormat("GetTaskProfileOutput; jobId = {{{0}}}, taskAttemptId = \"{1}\"", jobId, taskAttemptId);
            string jobDirectory = GetJobDirectory(jobId);
            string profileOutputFileName = System.IO.Path.Combine(jobDirectory, taskAttemptId.ToString() + "_profile.txt");
            if( System.IO.File.Exists(profileOutputFileName) )
            {
                using( System.IO.FileStream stream = System.IO.File.Open(profileOutputFileName, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite) )
                using( System.IO.StreamReader reader = new System.IO.StreamReader(stream) )
                {
                    return reader.ReadToEnd();
                }
            }
            return null;
        }

        public int GetTcpChannelPort(Guid jobId, TaskAttemptId taskAttemptId)
        {
            if( taskAttemptId == null )
                throw new ArgumentNullException("taskAttemptId");
            _log.DebugFormat("GetTcpChannelPort; jobId = {{{0}}}, taskId = \"{1}\"", jobId, taskAttemptId);
            return _taskRunner.GetTcpChannelPort(Job.CreateFullTaskId(jobId, taskAttemptId));
        }

        #endregion
        
        private void RunInternal()
        {
            _log.Info("-----Task server is starting-----");
            _log.LogEnvironmentInformation();
            _running = true;

            _taskRunner = new TaskRunner(this);

            LocalAddress = new ServerAddress(Dns.GetHostName(), Configuration.TaskServer.Port);

            AddDataForNextHeartbeat(new InitialStatusJetHeartbeatData() { MaxTasks = Configuration.TaskServer.MaxTasks, MaxNonInputTasks = Configuration.TaskServer.MaxNonInputTasks, FileServerPort = Configuration.TaskServer.FileServerPort });

            IPAddress[] addresses;
            if( System.Net.Sockets.Socket.OSSupportsIPv6 )
            {
                if (Configuration.TaskServer.ListenIPv4AndIPv6)
                    addresses = new[] { IPAddress.IPv6Any, IPAddress.Any };
                else
                    addresses = new[] { IPAddress.IPv6Any };
            }
            else
            {
                addresses = new[] { IPAddress.IPv6Any };
            }

            _fileServer = new FileChannelServer(this, addresses, Configuration.TaskServer.FileServerPort, Configuration.TaskServer.FileServerMaxConnections, Configuration.TaskServer.FileServerMaxIndexCacheSize);
            _fileServer.Start();

            while( _running )
            {
                SendHeartbeat();
                _taskRunner.KillTimedOutTasks();
                Thread.Sleep(_heartbeatInterval);
            }
        }

        private void ShutdownInternal()
        {
            _taskRunner.Stop();
            if( _fileServer != null )
                _fileServer.Stop();
            _running = false;
            RpcHelper.AbortRetries();
            _log.InfoFormat("-----Task server is shutting down-----");
        }

        private void SendHeartbeat()
        {
            JetHeartbeatData[] data = null;
            lock( _pendingHeartbeatData )
            {
                if( _pendingHeartbeatData.Count > 0 )
                {
                    data = _pendingHeartbeatData.ToArray();
                    _pendingHeartbeatData.Clear();
                }
            }

            JetHeartbeatResponse[] responses = null;

            RpcHelper.TryRemotingCall(() => responses = _jobServer.Heartbeat(LocalAddress, data), _heartbeatInterval, -1);

            if( responses != null )
                ProcessResponses(responses);
        }

        private void ProcessResponses(JetHeartbeatResponse[] responses)
        {
            foreach( var response in responses )
            {
                if( response.Command != TaskServerHeartbeatCommand.None )
                    _log.InfoFormat("Received {0} command.", response.Command);

                switch( response.Command )
                {
                case TaskServerHeartbeatCommand.ReportStatus:
                    AddDataForNextHeartbeat(new InitialStatusJetHeartbeatData() { MaxTasks = Configuration.TaskServer.MaxTasks, MaxNonInputTasks = Configuration.TaskServer.MaxNonInputTasks, FileServerPort = Configuration.TaskServer.FileServerPort });
                    break;
                case TaskServerHeartbeatCommand.RunTask:
                    RunTaskJetHeartbeatResponse runResponse = (RunTaskJetHeartbeatResponse)response;
                    _log.InfoFormat("Received run task command for task {{{0}}}_{1}.", runResponse.Job.JobId, runResponse.TaskAttemptId);
                    _taskRunner.AddTask(runResponse);
                    break;
                case TaskServerHeartbeatCommand.KillTask:
                    KillTaskJetHeartbeatResponse killResponse = (KillTaskJetHeartbeatResponse)response;
                    _log.InfoFormat("Received kill task command for task {{{0}}}_{1}.", killResponse.JobId, killResponse.TaskAttemptId);
                    _taskRunner.KillTask(killResponse);
                    break;
                case TaskServerHeartbeatCommand.CleanupJob:
                    CleanupJobJetHeartbeatResponse cleanupResponse = (CleanupJobJetHeartbeatResponse)response;
                    _log.InfoFormat("Received cleanup job command for job {{{0}}}.", cleanupResponse.JobId);
                    _taskRunner.CleanupJobTasks(cleanupResponse.JobId);
                    // Do file clean up asynchronously since it could take a long time.
                    ThreadPool.QueueUserWorkItem((state) => CleanupJobFiles((Guid)state), cleanupResponse.JobId);
                    break;
                }
            }
        }

        private void AddDataForNextHeartbeat(JetHeartbeatData data)
        {
            lock( _pendingHeartbeatData )
                _pendingHeartbeatData.Add(data);
        }

        private void CleanupJobFiles(Guid jobId, string directory)
        {
            foreach( string file in System.IO.Directory.GetFiles(directory) )
            {
                if( file.EndsWith(".output") || file.EndsWith(".input") )
                {
                    _log.DebugFormat("Job {0} cleanup: deleting file {1}.", jobId, file);
                    System.IO.File.Delete(file);
                }
            }
            foreach( string subDirectory in Directory.GetDirectories(directory) )
                CleanupJobFiles(jobId, subDirectory);
        }

        private void CleanupJobFiles(Guid jobId)
        {
            lock( _uncompressedTemporaryFileSizes )
            {
                _uncompressedTemporaryFileSizes.Remove(jobId);
            }

            try
            {
                if( Configuration.FileChannel.DeleteIntermediateFiles )
                {
                    string jobDirectory = GetJobDirectory(jobId);
                    CleanupJobFiles(jobId, jobDirectory);
                }
            }
            catch( UnauthorizedAccessException ex )
            {
                _log.Error("Failed to clean up job files", ex);
            }
            catch( IOException ex )
            {
                _log.Error("Failed to clean up job files", ex);
            }
        }
    }
}
