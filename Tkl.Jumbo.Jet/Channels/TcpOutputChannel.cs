using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Net.Sockets;
using System.Threading;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Represents the writing end of a TCP channel between two tasks.
    /// </summary>
    public sealed class TcpOutputChannel : OutputChannel
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TcpOutputChannel));

        private const int _retryDelay = 2000;

        /// <summary>
        /// Initializes a new instance of the <see cref="TcpOutputChannel"/> class.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
        public TcpOutputChannel(TaskExecutionUtility taskExecution)
            : base(taskExecution)
        {
        }

        /// <summary>
        /// Creates a <see cref="RecordWriter{T}"/> to which the channel can write its output.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="RecordWriter{T}"/> for the channel.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public override RecordWriter<T> CreateRecordWriter<T>()
        {
            List<RecordWriter<T>> writers = CreateOutputWriters<T>();
            if( writers.Count == 1 )
                return writers[0];
            else
                return CreateMultiRecordWriter(writers);
        }

        private List<RecordWriter<T>> CreateOutputWriters<T>()
            where T : IWritable, new()
        {
            List<RecordWriter<T>> writers = new List<RecordWriter<T>>(OutputTaskIds.Count);

            foreach( string taskId in OutputTaskIds )
            {
                TcpClient client = ConnectToOutput(taskId);
                writers.Add(new NetworkRecordWriter<T>(client, taskId));
            }

            return writers;
        }

        private TcpClient ConnectToOutput(string taskId)
        {
            _log.InfoFormat("Attempting to connect to output task {0}.", taskId);

            ServerAddress taskServer;
            do
            {
                taskServer = TaskExecution.JetClient.JobServer.GetTaskServerForTask(TaskExecution.Configuration.JobId, taskId);
                if( taskServer == null )
                {
                    _log.DebugFormat("Task {0} is not yet assigned to a server, waiting for retry...", taskId);
                    Thread.Sleep(_retryDelay);
                }
            } while( taskServer == null );

            _log.InfoFormat("Task {0} is running on task server {1}", taskId, taskServer);

            ITaskServerClientProtocol taskServerClient = JetClient.CreateTaskServerClient(taskServer);
            int port;
            do
            {
                port = taskServerClient.GetTcpChannelPort(TaskExecution.Configuration.JobId, taskId);
                if( port == 0 )
                {
                    _log.DebugFormat("Task {0} has not yet registered a port number, waiting for retry...", taskId);
                    Thread.Sleep(_retryDelay);
                }
            } while( port == 0 );

            _log.InfoFormat("Connecting to task {0} at TCP channel server {1}:{2}", taskId, taskServer.HostName, port);

            return new TcpClient(taskServer.HostName, port);
        }
    }
}
