// $Id$
//
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
using System.ComponentModel;

namespace TaskServerApplication
{
    sealed class TaskRunner
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskRunner));

        private Thread _taskStarterThread;
        private TaskServer _taskServer;
        private Queue<RunTaskJetHeartbeatResponse> _tasks = new Queue<RunTaskJetHeartbeatResponse>();
        private bool _running = true;
        private int _createProcessDelay;
        private readonly DfsClient _dfsClient;
        private readonly Dictionary<string, RunningTask> _runningTasks = new Dictionary<string, RunningTask>();
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
            lock( _tasks )
            {
                Monitor.Pulse(_tasks);
            }
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
                Monitor.Pulse(_tasks);
            }
        }

        public void KillTask(KillTaskJetHeartbeatResponse taskHeartbeat)
        {
            string fullTaskId = Job.CreateFullTaskId(taskHeartbeat.JobId, taskHeartbeat.TaskAttemptId);
            RunningTask task = null;
            lock( _runningTasks )
            {
                if( _runningTasks.TryGetValue(fullTaskId, out task) )
                {
                    _runningTasks.Remove(fullTaskId);
                    _log.InfoFormat("Killing task {{0}}_{1}.", taskHeartbeat.JobId, taskHeartbeat.TaskAttemptId);
                }
                else
                {
                    _log.WarnFormat("Task server received kill command for task {{0}}_{1} that wasn't running.", taskHeartbeat.JobId, taskHeartbeat.TaskAttemptId);
                    return;
                }
            }

            task.State = TaskAttemptStatus.Error;
            task.Kill();
        }

        public void KillTimedOutTasks()
        {
            lock( _runningTasks )
            {
                List<string> tasksToRemove = null;
                foreach( RunningTask task in _runningTasks.Values )
                {
                    if( task.IsTimedOut )
                    {
                        _log.WarnFormat("Task {0} has not reported progress for {1:0.0} seconds and is being killed.", task.FullTaskAttemptId, (DateTime.UtcNow - task.LastProgressTimeUtc).TotalSeconds);
                        if( tasksToRemove == null )
                            tasksToRemove = new List<string>();
                        tasksToRemove.Add(task.FullTaskAttemptId);
                        task.State = TaskAttemptStatus.Error;
                        task.Kill();
                        _taskServer.NotifyTaskStatusChanged(task.JobId, task.TaskAttemptId, TaskAttemptStatus.Error, null);
                    }
                }
                if( tasksToRemove != null )
                {
                    foreach( string task in tasksToRemove )
                        _runningTasks.Remove(task);
                }
            }
        }

        public void ReportProgress(string fullTaskAttemptId, TaskProgress progress)
        {
            if( fullTaskAttemptId == null )
                throw new ArgumentNullException("fullTaskAttemptId");

            lock( _runningTasks )
            {
                RunningTask task;
                if( _runningTasks.TryGetValue(fullTaskAttemptId, out task) && task.State == TaskAttemptStatus.Running )
                {
                    _log.InfoFormat("Task {0} progress: {1}", fullTaskAttemptId, progress);
                    task.LastProgressTimeUtc = DateTime.UtcNow;
                    _taskServer.NotifyTaskStatusChanged(task.JobId, task.TaskAttemptId, task.State, progress);
                }
                else
                    _log.WarnFormat("Received progress from task attempt {0} that was unknown or not running.", fullTaskAttemptId);
            }
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
                    _log.InfoFormat("Task {0} has completed successfully.", task.FullTaskAttemptId);
                    task.State = TaskAttemptStatus.Completed;
                    _taskServer.NotifyTaskStatusChanged(task.JobId, task.TaskAttemptId, task.State, null);
                }
                else
                    _log.WarnFormat("Task {0} was reported as completed but was not running.", fullTaskID);
            }
        }

        public void CleanupJobTasks(Guid jobID)
        {
            lock( _runningTasks )
            {
                string[] tasksToRemove = (from item in _runningTasks
                                          where item.Value.JobId == jobID
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

        public void RegisterTcpChannelPort(string fullTaskId, int port)
        {
            if( fullTaskId == null )
                throw new ArgumentNullException("fullTaskId");

            lock( _runningTasks )
            {
                RunningTask task = _runningTasks[fullTaskId];
                task.TcpChannelPort = port;
            }
        }

        public int GetTcpChannelPort(string fullTaskId)
        {
            if( fullTaskId == null )
                throw new ArgumentNullException("fullTaskId");

            lock( _runningTasks )
            {
                RunningTask task;
                if( _runningTasks.TryGetValue(fullTaskId, out task) )
                    return task.TcpChannelPort;
                else
                    return 0;
            }
        }

        private void TaskRunnerThread()
        {
            while( _running )
            {
                RunTaskJetHeartbeatResponse task = null;
                lock( _tasks )
                {
                    while( _tasks.Count == 0 && _running )
                        Monitor.Wait(_tasks);

                    if( !_running )
                        break;

                    task = _tasks.Dequeue();
                }
                if( task != null )
                {
                    RunTask(task);
                }
            }
        }

        private void RunTask(RunTaskJetHeartbeatResponse task)
        {
            _log.InfoFormat("Running task {{{0}}}_{1}.", task.Job.JobId, task.TaskAttemptId);
            string jobDirectory = _taskServer.GetJobDirectory(task.Job.JobId);
            JobConfiguration config;
            try
            {
                if( !(IO.Directory.Exists(jobDirectory) && _jobConfigurations.ContainsKey(task.Job.JobId)) )
                {
                    IO.Directory.CreateDirectory(jobDirectory);
                    _dfsClient.DownloadDirectory(task.Job.Path, jobDirectory);
                    string configPath = IO.Path.Combine(jobDirectory, "config");
                    IO.Directory.CreateDirectory(configPath);
                    _taskServer.Configuration.ToXml(IO.Path.Combine(configPath, "jet.config"));
                    _taskServer.DfsConfiguration.ToXml(IO.Path.Combine(configPath, "dfs.config"));
                    config = JobConfiguration.LoadXml(IO.Path.Combine(jobDirectory, Job.JobConfigFileName));
                    _jobConfigurations.Add(task.Job.JobId, config);
                }
                else
                    config = _jobConfigurations[task.Job.JobId];
            }
            catch( Exception ex )
            {
                _log.Error("Could not load job configuration.", ex);
                _taskServer.NotifyTaskStatusChanged(task.Job.JobId, task.TaskAttemptId, TaskAttemptStatus.Error, null);
                return;
            }
            RunningTask runningTask;
            lock( _runningTasks )
            {
                runningTask = new RunningTask(task.Job.JobId, jobDirectory, task.TaskAttemptId, task.Job.Path, config, _taskServer);
                runningTask.ProcessExited += new EventHandler(RunningTask_ProcessExited);
                _runningTasks.Add(runningTask.FullTaskAttemptId, runningTask);
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
                        _log.ErrorFormat("Task {0} did not complete sucessfully.", task.FullTaskAttemptId);
                        task.State = TaskAttemptStatus.Error;
                        _runningTasks.Remove(task.FullTaskAttemptId);
                        _taskServer.NotifyTaskStatusChanged(task.JobId, task.TaskAttemptId, task.State, null);
                        task.Dispose();
                    }
                    _log.InfoFormat("Task {0} has finished, state = {1}.", task.FullTaskAttemptId, task.State);
                }
            }
        }
    }
}
