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
    class TaskRunner
    {
        #region Nested types

        private sealed class RunningTask
        {
            private Process _process;
            private Thread _appDomainThread; // only used when running the task in an appdomain rather than a different process.

            public event EventHandler ProcessExited;

            public RunningTask(Guid jobID, string jobDirectory, string taskID)
            {
                JobID = jobID;
                TaskID = taskID;
                FullTaskID = string.Format("{{{0}}}_{1}", jobID, taskID);
                JobDirectory = jobDirectory;
                if( Debugger.IsAttached )
                    RunTaskAppDomain();
                else
                {
                    ProcessStartInfo startInfo = new ProcessStartInfo("TaskHost.exe", string.Format("\"{0}\" \"{1}\" \"{2}\"", jobID, jobDirectory, taskID));
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
                _appDomainThread = new Thread(RunTaskAppDomainThread);
                _appDomainThread.Name = FullTaskID;
                _appDomainThread.Start();
            }

            private void RunTaskAppDomainThread()
            {
                AppDomain taskDomain = AppDomain.CreateDomain(FullTaskID);
                taskDomain.ExecuteAssembly("TaskHost.exe", null, new[] { JobID.ToString(), JobDirectory, TaskID });
                AppDomain.Unload(taskDomain);
                OnProcessExited(EventArgs.Empty);
            }
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
                    task.Kill();
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
                }
                else
                    _log.WarnFormat("Task {0} was reported as completed but was not running.", task.FullTaskID);
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
                RunningTask runningTask = new RunningTask(task.Job.JobID, jobDirectory, task.TaskID);
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
                    }
                    _log.InfoFormat("Task {0} has finished, state = {1}.", task.FullTaskID, task.State);
                }
            }
        }
    }
}
