using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using IO = System.IO;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Dfs;
using System.Diagnostics;
using Tkl.Jumbo;

namespace TaskServerApplication
{
    sealed class TaskRunner
    {
        #region Nested types

        private sealed class RunningTask : IDisposable
        {
            private Process _process;
            private Thread _appDomainThread; // only used when running the task in an appdomain rather than a different process.
            private TaskServer _taskServer;

            public event EventHandler ProcessExited;

            public RunningTask(Guid jobID, string jobDirectory, string taskID, int attempt, string dfsJobDirectory, TaskConfiguration taskConfiguration, TaskServer taskServer)
            {
                JobID = jobID;
                TaskID = taskID;
                Attempt = attempt;
                FullTaskID = Job.CreateFullTaskID(jobID, taskID);
                JobDirectory = jobDirectory;
                DfsJobDirectory = dfsJobDirectory;
                _taskServer = taskServer;
                TaskConfiguration = taskConfiguration;
            }

            public TaskAttemptStatus State { get; set; }

            public Guid JobID { get; private set; }

            public string TaskID { get; private set; }

            public string JobDirectory { get; private set; }

            public string FullTaskID { get; private set; }

            public string DfsJobDirectory { get; private set; }

            public int Attempt { get; private set; }

            public TaskConfiguration TaskConfiguration { get; private set; }

            public void Run(int createProcessDelay)
            {
                if( Debugger.IsAttached )
                    RunTaskAppDomain();
                else
                {
                    _log.DebugFormat("Launching new process for task {0}.", FullTaskID);
                    ProcessStartInfo startInfo = new ProcessStartInfo("TaskHost.exe", string.Format("\"{0}\" \"{1}\" \"{2}\" \"{3}\" {4} {5} {6} {7} {8} {9}", JobID, JobDirectory, TaskID, DfsJobDirectory, _taskServer.Configuration.TaskServer.Port, _taskServer.Configuration.JobServer.HostName, _taskServer.Configuration.JobServer.Port, _taskServer.DfsConfiguration.NameServer.HostName, _taskServer.DfsConfiguration.NameServer.Port, Attempt));
                    startInfo.UseShellExecute = false;
                    startInfo.CreateNoWindow = true;
                    string profileOutputFile = null;
                    if( !string.IsNullOrEmpty(TaskConfiguration.ProfileOptions) )
                    {
                        profileOutputFile = IO.Path.Combine(JobDirectory, string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}_{1}_profile.txt", TaskID, Attempt));
                        if( RuntimeEnvironment.RuntimeType == RuntimeEnvironmentType.Mono )
                            _log.InfoFormat("Profiling is enabled for task {0}, output file {1}.", FullTaskID, profileOutputFile);
                        else
                            _log.WarnFormat("Profiling is requested for task {0}, but not supported on this platform.", FullTaskID);
                    }
                    RuntimeEnvironment.ModifyProcessStartInfo(startInfo, profileOutputFile, TaskConfiguration.ProfileOptions);
                    _process = new Process();
                    _process.StartInfo = startInfo;
                    _process.EnableRaisingEvents = true;
                    _process.Exited += new EventHandler(_process_Exited);
                    _process.Start();
                    _log.DebugFormat("Host process for task {0} has started, pid = {1}.", FullTaskID, _process.Id);
                    if( createProcessDelay > 0 )
                    {
                        _log.DebugFormat("Sleeping for {0}ms", createProcessDelay);
                        Thread.Sleep(createProcessDelay);
                    }
                }
                State = TaskAttemptStatus.Running;
            }

            public void Kill()
            {
                if( Debugger.IsAttached )
                    _appDomainThread.Abort();
                else
                    _process.Kill();
            }

            private void OnProcessExited(EventArgs e)
            {
                EventHandler handler = ProcessExited;
                if( handler != null )
                    handler(this, e);
            }

            private void _process_Exited(object sender, EventArgs e)
            {
                OnProcessExited(EventArgs.Empty);
            }

            private void RunTaskAppDomain()
            {
                _log.DebugFormat("Running task {0} in an AppDomain.", FullTaskID);
                _appDomainThread = new Thread(RunTaskAppDomainThread);
                _appDomainThread.Name = FullTaskID;
                _appDomainThread.Start();
            }

            private void RunTaskAppDomainThread()
            {
                AppDomainSetup setup = new AppDomainSetup();
                setup.ApplicationBase = Environment.CurrentDirectory;
                AppDomain taskDomain = AppDomain.CreateDomain(FullTaskID, null, setup);
                try
                {
                    taskDomain.ExecuteAssembly("TaskHost.exe", null, new string[] { JobID.ToString(), JobDirectory, TaskID, DfsJobDirectory, _taskServer.Configuration.TaskServer.Port.ToString(), _taskServer.Configuration.JobServer.HostName, _taskServer.Configuration.JobServer.Port.ToString(), _taskServer.DfsConfiguration.NameServer.HostName, _taskServer.DfsConfiguration.NameServer.Port.ToString(), Attempt.ToString() });
                }
                catch( Exception ex )
                {
                    _log.Error(string.Format("Error running task {0} in task domain", FullTaskID), ex);
                }
                finally
                {
                    AppDomain.Unload(taskDomain);
                }
                OnProcessExited(EventArgs.Empty);
            }

            #region IDisposable Members

            public void Dispose()
            {
                if( _process != null )
                {
                    _process.Dispose();
                    _process = null;
                }
                GC.SuppressFinalize(this);
            }

            #endregion
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskRunner));

        private Thread _taskStarterThread;
        private TaskServer _taskServer;
        private AutoResetEvent _taskAddedEvent = new AutoResetEvent(false);
        private Queue<RunTaskJetHeartbeatResponse> _tasks = new Queue<RunTaskJetHeartbeatResponse>();
        private bool _running = true;
        private int _createProcessDelay;
        private readonly DfsClient _dfsClient;
        private readonly Dictionary<string, RunningTask> _runningTasks = new Dictionary<string,RunningTask>();
        private readonly Dictionary<Guid, JobConfiguration> _jobConfigurations = new Dictionary<Guid, JobConfiguration>();

        public TaskRunner(TaskServer taskServer)
        {
            if( taskServer == null )
                throw new ArgumentNullException("taskServer");
            _taskServer = taskServer;
            _createProcessDelay = _taskServer.Configuration.TaskServer.ProcessCreationDelay;
            _dfsClient = new DfsClient(taskServer.DfsConfiguration);
            _taskStarterThread = new Thread(TaskRunnerThread);
            _taskStarterThread.IsBackground = true;
            _taskStarterThread.Name = "TaskStarter";
            _taskStarterThread.Start();
        }

        public void Stop()
        {
            _running = false;
            _taskAddedEvent.Set();
            _taskStarterThread.Join();
            lock( _runningTasks )
            {
                foreach( RunningTask task in _runningTasks.Values )
                {
                    if( task.State == TaskAttemptStatus.Running )
                        task.Kill();
                }
            }
        }

        public void AddTask(RunTaskJetHeartbeatResponse task)
        {
            lock( _tasks )
            {
                _tasks.Enqueue(task);
            }
            _taskAddedEvent.Set();
        }

        public void ReportCompletion(string fullTaskID)
        {
            if( fullTaskID == null )
                throw new ArgumentNullException("fullTaskID");

            lock( _runningTasks )
            {
                RunningTask task;
                if( _runningTasks.TryGetValue(fullTaskID, out task) && task.State == TaskAttemptStatus.Running )
                {
                    _log.InfoFormat("Task {0} has completed successfully.", task.FullTaskID);
                    task.State = TaskAttemptStatus.Completed;
                    _taskServer.NotifyTaskStatusChanged(task.JobID, task.TaskID, task.State);
                }
                else
                    _log.WarnFormat("Task {0} was reported as completed but was not running.", task.FullTaskID);
            }
        }

        public void CleanupJobTasks(Guid jobID)
        {
            lock( _runningTasks )
            {
                string[] tasksToRemove = (from item in _runningTasks
                                          where item.Value.JobID == jobID
                                          select item.Key).ToArray();
                foreach( string task in tasksToRemove )
                {
                    if( _runningTasks[task].State == TaskAttemptStatus.Running )
                    {
                        _log.WarnFormat("Received cleanup command for still running task {0} (this usually means the job failed).", task);
                        _runningTasks[task].Kill();
                    }
                    _log.InfoFormat("Removing data pertaining to task {0}.", task);
                    _runningTasks[task].Dispose();
                    _runningTasks.Remove(task);
                }
            }
        }

        public TaskAttemptStatus GetTaskStatus(string fullTaskID)
        {
            if( fullTaskID == null )
                throw new ArgumentNullException("fullTaskID");

            lock( _runningTasks )
            {
                RunningTask task;
                if( _runningTasks.TryGetValue(fullTaskID, out task) )
                {
                    return task.State;
                }
                else
                    return TaskAttemptStatus.NotStarted;
            }
        }

        public string GetJobDirectory(string fullTaskID)
        {
            if( fullTaskID == null )
                throw new ArgumentNullException("fullTaskID");

            lock( _runningTasks )
            {
                RunningTask task = _runningTasks[fullTaskID];
                return task.JobDirectory;
            }
        }

        private void TaskRunnerThread()
        {
            while( _running )
            {
                RunTaskJetHeartbeatResponse task = null;
                lock( _tasks )
                {
                    if( _tasks.Count > 0 )
                    {
                        task = _tasks.Dequeue();
                    }
                }
                if( task != null )
                {
                    RunTask(task);
                }
                else
                    _taskAddedEvent.WaitOne();
            }
        }

        private void RunTask(RunTaskJetHeartbeatResponse task)
        {
            _log.InfoFormat("Running task {{{0}}}_{1}.", task.Job.JobID, task.TaskID);
            string jobDirectory = _taskServer.GetJobDirectory(task.Job.JobID);
            JobConfiguration config;
            if( !IO.Directory.Exists(jobDirectory) )
            {
                IO.Directory.CreateDirectory(jobDirectory);
                _dfsClient.DownloadDirectory(task.Job.Path, jobDirectory);
                config = JobConfiguration.LoadXml(IO.Path.Combine(jobDirectory, Job.JobConfigFileName));
                _jobConfigurations.Add(task.Job.JobID, config);
            }
            else
                config = _jobConfigurations[task.Job.JobID];
            TaskConfiguration taskConfig = config.GetTask(task.TaskID);
            RunningTask runningTask;
            lock( _runningTasks )
            {
                runningTask = new RunningTask(task.Job.JobID, jobDirectory, task.TaskID, task.Attempt, task.Job.Path, taskConfig, _taskServer);
                runningTask.ProcessExited += new EventHandler(RunningTask_ProcessExited);
                _runningTasks.Add(runningTask.FullTaskID, runningTask);
            }
            runningTask.Run(_createProcessDelay);
        }

        private void RunningTask_ProcessExited(object sender, EventArgs e)
        {
            if( _running )
            {
                RunningTask task = (RunningTask)sender;
                lock( _runningTasks )
                {
                    if( task.State != TaskAttemptStatus.Completed )
                    {
                        _log.ErrorFormat("Task {0} did not complete sucessfully.", task.FullTaskID);
                        task.State = TaskAttemptStatus.Error;
                        _runningTasks.Remove(task.FullTaskID);
                        _taskServer.NotifyTaskStatusChanged(task.JobID, task.TaskID, task.State);
                        task.Dispose();
                    }
                    _log.InfoFormat("Task {0} has finished, state = {1}.", task.FullTaskID, task.State);
                }
            }
        }
    }
}
