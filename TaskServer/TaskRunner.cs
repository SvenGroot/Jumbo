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
        private class RunningTask
        {
            private Process _process;
            private string _logFile;
            private object _logFileLock = new object();

            public event EventHandler ProcessExited;

            public RunningTask(Guid jobID, string jobDirectory, string taskID)
            {
                JobID = jobID;
                TaskID = taskID;
                ProcessStartInfo startInfo = new ProcessStartInfo("TaskHost.exe", string.Format("\"{0}\" \"{1}\"", jobDirectory, taskID));
                startInfo.RedirectStandardError = true;
                startInfo.RedirectStandardOutput = true;
                startInfo.UseShellExecute = false;
                startInfo.CreateNoWindow = true;
                _logFile = IO.Path.Combine(jobDirectory, taskID + ".log");
                RuntimeEnvironment.ModifyProcessStartInfo(startInfo);
                _process = new Process();
                _process.StartInfo = startInfo;
                _process.EnableRaisingEvents = true;
                _process.Exited += new EventHandler(_process_Exited);
                _process.OutputDataReceived += new DataReceivedEventHandler(_process_OutputDataReceived);
                _process.ErrorDataReceived += new DataReceivedEventHandler(_process_OutputDataReceived);
                _process.Start();
                _process.BeginErrorReadLine();
                _process.BeginOutputReadLine();
                _log.DebugFormat("Host process for task {{{0}}}_{1} has started.", jobID, taskID);
            }

            public Guid JobID { get; private set; }

            public string TaskID { get; private set; }

            public void Kill()
            {
                _process.Kill();
            }

            protected virtual void OnProcessExited(EventArgs e)
            {
                EventHandler handler = ProcessExited;
                if( handler != null )
                    handler(this, e);
            }

            void _process_Exited(object sender, EventArgs e)
            {
                OnProcessExited(EventArgs.Empty);
            }

            void _process_OutputDataReceived(object sender, DataReceivedEventArgs e)
            {
                lock( _logFileLock )
                {
                    using( IO.StreamWriter writer = new System.IO.StreamWriter(_logFile, true) )
                    {
                        writer.WriteLine(e.Data);
                    }
                }
            }
        }

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskRunner));

        private Thread _taskStarterThread;
        private TaskServer _taskServer;
        private AutoResetEvent _taskAddedEvent = new AutoResetEvent(false);
        private Queue<RunTaskJetHeartbeatResponse> _tasks = new Queue<RunTaskJetHeartbeatResponse>();
        private bool _running = true;
        private readonly DfsClient _dfsClient;
        private readonly List<RunningTask> _runningTasks = new List<RunningTask>();

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
                foreach( RunningTask task in _runningTasks )
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
                _runningTasks.Add(runningTask);
            }
        }

        private void RunningTask_ProcessExited(object sender, EventArgs e)
        {
            if( _running )
            {
                RunningTask task = (RunningTask)sender;
                lock( _runningTasks )
                {
                    _runningTasks.Remove(task);
                }
                _log.InfoFormat("Task {{{0}}}_{1} has finished.", task.JobID, task.TaskID);
            }
        }
    }
}
