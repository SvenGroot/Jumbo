using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using IO = System.IO;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Dfs;
using System.Diagnostics;

namespace TaskServerApplication
{
    class TaskRunner
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskRunner));

        private Thread _taskStarterThread;
        private TaskServer _taskServer;
        private AutoResetEvent _taskAddedEvent = new AutoResetEvent(false);
        private Queue<RunTaskJetHeartbeatResponse> _tasks = new Queue<RunTaskJetHeartbeatResponse>();
        private bool _running = true;
        private readonly DfsClient _dfsClient;

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
        }
    }
}
