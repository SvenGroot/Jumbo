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

            public RunningTask(Guid jobID, string jobDirectory, string taskID, string dfsJobDirectory, TaskServer taskServer)
            {
                JobID = jobID;
                TaskID = taskID;
                FullTaskID = Job.CreateFullTaskID(jobID, taskID);
                JobDirectory = jobDirectory;
                DfsJobDirectory = dfsJobDirectory;
                _taskServer = taskServer;
                if( Debugger.IsAttached )
                    RunTaskAppDomain();
                else
                {
                    _log.DebugFormat("Launching new process for task {0}.", FullTaskID);
                    ProcessStartInfo startInfo = new ProcessStartInfo("TaskHost.exe", string.Format("\"{0}\" \"{1}\" \"{2}\" \"{3}\" {4} {5} {6} {7} {8}", jobID, jobDirectory, taskID, dfsJobDirectory, taskServer.Configuration.TaskServer.Port, taskServer.Configuration.JobServer.HostName, taskServer.Configuration.JobServer.Port, taskServer.DfsConfiguration.NameServer.HostName, taskServer.DfsConfiguration.NameServer.Port));
                    startInfo.UseShellExecute = false;
                    startInfo.CreateNoWindow = true;
                    RuntimeEnvironment.ModifyProcessStartInfo(startInfo);
                    _process = new Process();
                    _process.StartInfo = startInfo;
                    _process.EnableRaisingEvents = true;
                    _process.Exited += new EventHandler(_process_Exited);
                    _process.Start();
                }
                State = TaskStatus.Running;
                _log.DebugFormat("Host process for task {0} has started.", FullTaskID);
            }

            public TaskStatus State { get; set; }

            public Guid JobID { get; private set; }

            public string TaskID { get; private set; }

            public string JobDirectory { get; private set; }

            public string FullTaskID { get; private set; }

            public string DfsJobDirectory { get; private set; }

            public void Kill()
            {
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
                taskDomain.ExecuteAssembly("TaskHost.exe", null, new string[] { JobID.ToString(), JobDirectory, TaskID, DfsJobDirectory, _taskServer.Configuration.TaskServer.Port.ToString(), _taskServer.Configuration.JobServer.HostName, _taskServer.Configuration.JobServer.Port.ToString(), _taskServer.DfsConfiguration.NameServer.HostName, _taskServer.DfsConfiguration.NameServer.Port.ToString() });
                AppDomain.Unload(taskDomain);
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
        private readonly DfsClient _dfsClient;
        private readonly Dictionary<string, RunningTask> _runningTasks = new Dictionary<string,RunningTask>();

        public TaskRunner(TaskServer taskServer)
        {
            if( taskServer == null )
                throw new ArgumentNullException("taskServer");
            _taskServer = taskServer;
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
                    if( task.State == TaskStatus.Running )
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
                if( _runningTasks.TryGetValue(fullTaskID, out task) && task.State == TaskStatus.Running )
                {
                    _log.InfoFormat("Task {0} has completed successfully.", task.FullTaskID);
                    task.State = TaskStatus.Completed;
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
                    Debug.Assert(_runningTasks[task].State > TaskStatus.Running);
                    _log.InfoFormat("Removing data pertaining to task {0}.", task);
                    _runningTasks[task].Dispose();
                    _runningTasks.Remove(task);
                }
            }
        }

        public TaskStatus GetTaskStatus(string fullTaskID)
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
                    return TaskStatus.NotStarted;
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
            string jobDirectory = IO.Path.Combine(_taskServer.Configuration.TaskServer.TaskDirectory, "job_" + task.Job.JobID.ToString());
            if( !IO.Directory.Exists(jobDirectory) )
            {
                IO.Directory.CreateDirectory(jobDirectory);
                _dfsClient.DownloadDirectory(task.Job.Path, jobDirectory);
            }
            lock( _runningTasks )
            {
                RunningTask runningTask = new RunningTask(task.Job.JobID, jobDirectory, task.TaskID, task.Job.Path, _taskServer);
                runningTask.ProcessExited += new EventHandler(RunningTask_ProcessExited);
                _runningTasks.Add(runningTask.FullTaskID, runningTask);
            }
        }

        private void RunningTask_ProcessExited(object sender, EventArgs e)
        {
            if( _running )
            {
                RunningTask task = (RunningTask)sender;
                lock( _runningTasks )
                {
                    if( task.State != TaskStatus.Completed )
                    {
                        _log.ErrorFormat("Task {0} did not complete sucessfully.", task.FullTaskID);
                        task.State = TaskStatus.Error;
                        _taskServer.NotifyTaskStatusChanged(task.JobID, task.TaskID, task.State);
                    }
                    _log.InfoFormat("Task {0} has finished, state = {1}.", task.FullTaskID, task.State);
                }
            }
        }
    }
}
