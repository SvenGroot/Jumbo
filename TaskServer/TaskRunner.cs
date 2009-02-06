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

        private sealed class TaskHostProcess : IDisposable
        {
            private Process _process;
            private Thread _appDomainThread; // only used when running the task hosts in an appdomain rather than a different process.
            private TaskRunner _taskRunner;

            public TaskHostProcess(TaskRunner taskRunner, int instanceId)
            {
                InstanceId = instanceId;
                _taskRunner = taskRunner;
                RunTaskHost();
            }

            public int InstanceId { get; private set; }

            public RunningTask CurrentRunningTask { get; set; }

            public void Kill()
            {
                CurrentRunningTask = null;
                if( Debugger.IsAttached )
                    _appDomainThread.Abort();
                else
                    _process.Kill();
            }

            private void RunTaskHost()
            {
                if( _taskRunner.IsRunning )
                {
                    if( Debugger.IsAttached )
                        RunTaskHostAppDomain();
                    else
                    {
                        _log.InfoFormat("Launching new process for task host {0}.", InstanceId);
                        ProcessStartInfo startInfo = new ProcessStartInfo("TaskHost.exe", string.Format("{0} {1} {2} {3} {4} {5}", InstanceId, _taskRunner.TaskServer.Configuration.TaskServer.Port, _taskRunner.TaskServer.Configuration.JobServer.HostName, _taskRunner.TaskServer.Configuration.JobServer.Port, _taskRunner.TaskServer.DfsConfiguration.NameServer.HostName, _taskRunner.TaskServer.DfsConfiguration.NameServer.Port));
                        startInfo.UseShellExecute = false;
                        startInfo.CreateNoWindow = true;
                        RuntimeEnvironment.ModifyProcessStartInfo(startInfo, null, null);
                        _process = new Process();
                        _process.StartInfo = startInfo;
                        _process.EnableRaisingEvents = true;
                        _process.Exited += new EventHandler(_process_Exited);
                        _process.Start();
                        _log.DebugFormat("Task host {0} process has started, pid = {1}.", InstanceId, _process.Id);
                        int createProcessDelay = _taskRunner.TaskServer.Configuration.TaskServer.ProcessCreationDelay;
                        if( createProcessDelay > 0 )
                        {
                            _log.DebugFormat("Sleeping for {0}ms", createProcessDelay);
                            Thread.Sleep(createProcessDelay);
                        }
                    }
                }
            }

            private void RunTaskHostAppDomain()
            {
                _log.InfoFormat("Starting task host {0} in an AppDomain.", InstanceId);
                _appDomainThread = new Thread(RunTaskHostAppDomainThread);
                _appDomainThread.Name = "TaskHost" + InstanceId.ToString();
                _appDomainThread.Start();
            }

            private void RunTaskHostAppDomainThread()
            {
                AppDomainSetup setup = new AppDomainSetup();
                setup.ApplicationBase = Environment.CurrentDirectory;
                AppDomain taskDomain = AppDomain.CreateDomain("TaskHost" + InstanceId.ToString(), null, setup);
                try
                {
                    taskDomain.ExecuteAssembly("TaskHost.exe", null, new string[] { InstanceId.ToString(), _taskRunner.TaskServer.Configuration.TaskServer.Port.ToString(), _taskRunner.TaskServer.Configuration.JobServer.HostName, _taskRunner.TaskServer.Configuration.JobServer.Port.ToString(), _taskRunner.TaskServer.DfsConfiguration.NameServer.HostName, _taskRunner.TaskServer.DfsConfiguration.NameServer.Port.ToString() });
                }
                catch( Exception ex )
                {
                    _log.Error("Error running task host in task domain", ex);
                }
                finally
                {
                    AppDomain.Unload(taskDomain);
                }
                _log.InfoFormat("Task host {0} app domain has unloaded.", InstanceId);
                OnProcessExited();
                RunTaskHost();
            }

            private void _process_Exited(object sender, EventArgs e)
            {
                _log.InfoFormat("Process for task host {0} has exited.", InstanceId);
                OnProcessExited();
                RunTaskHost();
            }

            private void OnProcessExited()
            {
                RunningTask task = CurrentRunningTask;
                if( task != null )
                    task.NotifyProcessExit();
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

        private sealed class RunningTask
        {
            public event EventHandler TaskTerminated;

            public RunningTask(TaskExecutionInfo task, TaskHostProcess process)
            {
                Debug.Assert(process.CurrentRunningTask == null);
                process.CurrentRunningTask = this;
                Process = process;
                TaskInfo = task;
                FullTaskId = Job.CreateFullTaskID(task.JobId, task.TaskId);
            }

            public TaskExecutionInfo TaskInfo { get; private set; }

            public TaskAttemptStatus State { get; set; }

            public TaskHostProcess Process { get; private set; }

            public string FullTaskId { get; private set; }

            public void NotifyProcessExit()
            {
                OnTaskTerminated(EventArgs.Empty);
            }

            private void OnTaskTerminated(EventArgs e)
            {
                EventHandler handler = TaskTerminated;
                if( handler != null )
                    handler(this, e);
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskRunner));

        private Queue<RunTaskJetHeartbeatResponse> _tasks = new Queue<RunTaskJetHeartbeatResponse>();
        private volatile bool _running = true;
        private readonly DfsClient _dfsClient;
        private readonly Dictionary<string, RunningTask> _runningTasks = new Dictionary<string,RunningTask>();
        private TaskHostProcess[] _processes;
        private ManualResetEvent _taskAddedEvent = new ManualResetEvent(false);
        private EventHandler _taskTerminatedHandler;

        public TaskRunner(TaskServer taskServer)
        {
            if( taskServer == null )
                throw new ArgumentNullException("taskServer");
            TaskServer = taskServer;
            _dfsClient = new DfsClient(taskServer.DfsConfiguration);

            _taskTerminatedHandler = new EventHandler(RunningTask_TaskTerminated);
            _processes = new TaskHostProcess[taskServer.Configuration.TaskServer.MaxTasks * 2];
            for( int x = 0; x < _processes.Length; ++x )
            {
                _processes[x] = new TaskHostProcess(this, x);
            }
        }

        public TaskServer TaskServer { get; private set; }

        public bool IsRunning { get { return _running; } }

        public void Stop()
        {
            _running = false;
            _taskAddedEvent.Set();
            lock( _runningTasks )
            {
                foreach( TaskHostProcess process in _processes )
                {
                    process.Kill();
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
                    _log.InfoFormat("Task {0} has completed successfully.", task.FullTaskId);
                    task.State = TaskAttemptStatus.Completed;
                    TaskServer.NotifyTaskStatusChanged(task.TaskInfo.JobId, task.TaskInfo.TaskId, task.State);
                }
                else
                    _log.WarnFormat("Task {0} was reported as completed but was not running.", task.FullTaskId);
            }
        }

        public void CleanupJobTasks(Guid jobID)
        {
            lock( _runningTasks )
            {
                string[] tasksToRemove = (from item in _runningTasks
                                          where item.Value.TaskInfo.JobId == jobID
                                          select item.Key).ToArray();
                foreach( string task in tasksToRemove )
                {
                    _runningTasks[task].TaskTerminated -= _taskTerminatedHandler;
                    if( _runningTasks[task].State == TaskAttemptStatus.Running )
                    {
                        _log.WarnFormat("Received cleanup command for still running task {0} (this usually means the job failed).", task);
                        _runningTasks[task].Process.Kill(); // the process will automatically restart.
                    }
                    _log.InfoFormat("Removing data pertaining to task {0}.", task);
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
                return task.TaskInfo.JobDirectory;
            }
        }

        public TaskExecutionInfo WaitForTask(int instanceId, int timeout)
        {
            if( instanceId < 0 || instanceId >= _processes.Length )
                throw new ArgumentOutOfRangeException("instanceId", "Invalid instance id");

            _log.InfoFormat("Task host {0} is waiting for tasks.", instanceId);
            TaskHostProcess process = _processes[instanceId];
            lock( process )
            {
                // This function called means the task host isn't running anything, so we need to reset CurrentRunningTask
                if( process.CurrentRunningTask != null )
                {
                    process.CurrentRunningTask.NotifyProcessExit();
                }
                process.CurrentRunningTask = null;
            }

            RunTaskJetHeartbeatResponse taskResponse = null;
            _taskAddedEvent.WaitOne(timeout, false);

            if( !_running )
                throw new ServerShutdownException("Task server is shut down");

            lock( _tasks )
            {
                if( _tasks.Count > 0 )
                    taskResponse = _tasks.Dequeue();
                if( _tasks.Count == 0 )
                    _taskAddedEvent.Reset();
            }
            if( taskResponse != null )
                return RunTask(instanceId, taskResponse);
            else
                return null;
        }


        private TaskExecutionInfo RunTask(int instanceId, RunTaskJetHeartbeatResponse task)
        {

            _log.InfoFormat("Running task {{{0}}}_{1} in task host {2}.", task.Job.JobID, task.TaskID, instanceId);
            string jobDirectory = TaskServer.GetJobDirectory(task.Job.JobID);
            lock( _tasks )
            {
                if( !IO.Directory.Exists(jobDirectory) )
                {
                    IO.Directory.CreateDirectory(jobDirectory);
                    _dfsClient.DownloadDirectory(task.Job.Path, jobDirectory);
                }
            }

            TaskExecutionInfo taskInfo = new TaskExecutionInfo()
            {
                JobId = task.Job.JobID,
                TaskId = task.TaskID,
                JobDirectory = jobDirectory,
                DfsJobDirectory = task.Job.Path,
                Attempt = task.Attempt
            };

            RunningTask runningTask;
            lock( _runningTasks )
            {
                runningTask = new RunningTask(taskInfo, _processes[instanceId]);
                runningTask.State = TaskAttemptStatus.Running;
                runningTask.TaskTerminated += _taskTerminatedHandler;
                _runningTasks.Add(runningTask.FullTaskId, runningTask);
            }
            return taskInfo;
        }

        private void RunningTask_TaskTerminated(object sender, EventArgs e)
        {
            if( _running )
            {
                RunningTask task = (RunningTask)sender;
                lock( _runningTasks )
                {
                    if( task.State != TaskAttemptStatus.Completed )
                    {
                        _log.ErrorFormat("Task {0} did not complete sucessfully.", task.FullTaskId);
                        task.State = TaskAttemptStatus.Error;
                        _runningTasks.Remove(task.FullTaskId);
                        task.TaskTerminated -= _taskTerminatedHandler;
                        TaskServer.NotifyTaskStatusChanged(task.TaskInfo.JobId, task.TaskInfo.TaskId, task.State);
                    }
                    _log.InfoFormat("Task {0} has finished, state = {1}.", task.FullTaskId, task.State);
                }
            }
        }
    }
}
