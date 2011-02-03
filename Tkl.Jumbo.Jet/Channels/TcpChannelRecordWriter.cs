using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Net.Sockets;
using System.Collections.ObjectModel;
using System.Threading;

namespace Tkl.Jumbo.Jet.Channels
{
    sealed class TcpChannelRecordWriter<T> : SpillRecordWriter<T>
    {
        #region Nested types

        private struct TaskConnectionInfo : IDisposable
        {
            public string HostName { get; set; }
            public int Port { get; set; }
            public int[] Partitions { get; set; }
            public TcpClient Client { get; set; }
            public WriteBufferedStream ClientStream { get; set; }

            public void Dispose()
            {
                if( ClientStream != null )
                    ClientStream.Dispose();
                if( Client != null )
                    ((IDisposable)Client).Dispose();
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TcpOutputChannel));
        private const int _retryDelay = 2000;

        private readonly TaskId[] _outputIds;
        private readonly int _partitions;
        private readonly bool _reuseConnections;
        private readonly byte[] _header = new byte[TcpInputChannel.HeaderSize];
        private readonly TaskConnectionInfo[] _taskConnections;
        private readonly TaskExecutionUtility _taskExecution;
        private bool _hasFinalSpill;
        private bool _disposed;

        public TcpChannelRecordWriter(ReadOnlyCollection<string> outputIds, TaskExecutionUtility taskExecution, bool reuseConnections, IPartitioner<T> partitioner, int bufferSize, int limit)
            : base(partitioner, bufferSize, limit)
        {
            _outputIds = outputIds.Select(t => new TaskId(t)).ToArray();
            _partitions = partitioner.Partitions;
            _taskConnections = new TaskConnectionInfo[outputIds.Count];
            _taskExecution = taskExecution;
            if( reuseConnections )
                _header[0] = (byte)TcpChannelConnectionFlags.KeepAlive;
            WriteInt32ToHeader(1, taskExecution.Context.TaskAttemptId.TaskId.TaskNumber);
        }

        protected override void SpillOutput(bool finalSpill)
        {
            SendSegments(finalSpill, true);
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if( !_disposed )
            {
                _disposed = true;
                if( !_hasFinalSpill )
                {
                    SendSegments(true, false); // Send empty partitions to all tasks to signify the end of writing
                }

                if( disposing )
                {
                    for( int x = 0; x < _taskConnections.Length; ++x )
                        _taskConnections[x].Dispose();
                }
            }
        }

        private void SendSegments(bool finalSpill, bool sendData)
        {
            WriteInt32ToHeader(9, sendData ? SpillCount : (SpillCount + 1));
            if( finalSpill )
            {
                _hasFinalSpill = true;
                _header[0] |= (byte)TcpChannelConnectionFlags.FinalSegment;
            }

            for( int taskIndex = 0; taskIndex < _outputIds.Length; ++taskIndex )
            {
                SendSegmentToTask(finalSpill, sendData, taskIndex);
            }
        }

        private void SendSegmentToTask(bool finalSpill, bool sendData, int taskIndex)
        {
            bool disposeStream = false;
            WriteBufferedStream stream = _taskConnections[taskIndex].ClientStream;
            TcpClient client = null;
            try
            {
                // TODO: Although is built with support for multiple partitions per task in mind, the receiving side currently does not support it.
                int[] partitions = _taskConnections[taskIndex].Partitions;
                if( partitions == null )
                {
                    partitions = _taskExecution.Context.StageConfiguration.OutputChannel.PartitionsPerTask <= 1 ? new[] { _outputIds[taskIndex].TaskNumber } : _taskExecution.JobServerTaskClient.GetPartitionsForTask(_taskExecution.Context.JobId, _outputIds[taskIndex]);
                    _taskConnections[taskIndex].Partitions = partitions;
                }

                foreach( int partition in partitions )
                {
                    int size = SpillDataSizeForPartition(partition);
                    // Always send a final spill, even if its empty.
                    if( finalSpill || size > 0 )
                    {
                        if( stream == null )
                        {
                            disposeStream = true;
                            client = ConnectToTask(taskIndex);
                            stream = new WriteBufferedStream(client.GetStream());
                            if( _reuseConnections )
                            {
                                _taskConnections[taskIndex].Client = client;
                                _taskConnections[taskIndex].ClientStream = stream;
                                disposeStream = false;
                            }
                        }

                        WriteInt32ToHeader(5, size);
                        stream.Write(_header, 0, _header.Length);
                        if( sendData )
                            WritePartition(partition, stream, null);
                        stream.Flush();
                    }
                }

            }
            finally
            {
                if( disposeStream )
                {
                    if( stream != null )
                        stream.Dispose();
                    if( client != null )
                        ((IDisposable)client).Dispose();
                }
            }
        }

        private void WriteInt32ToHeader(int index, int value)
        {
            _header[index] = (byte)(value & 0xFF);
            _header[index + 1] = (byte)((value >> 8) & 0xFF);
            _header[index + 2] = (byte)((value >> 16) & 0xFF);
            _header[index + 3] = (byte)((value >> 24) & 0xFF);
        }

        private TcpClient ConnectToTask(int taskIndex)
        {
            if( _taskConnections[taskIndex].HostName == null )
            {
                TaskId taskId = _outputIds[taskIndex];
                ServerAddress taskServer;
                do
                {
                    taskServer = _taskExecution.JetClient.JobServer.GetTaskServerForTask(_taskExecution.Context.JobId, taskId.ToString());
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
                    // Since a task failure fails the job with the TCP channel, the attempt number will always be 1.
                    port = taskServerClient.GetTcpChannelPort(_taskExecution.Context.JobId, new TaskAttemptId(taskId, 1));
                    if( port == 0 )
                    {
                        _log.DebugFormat("Task {0} has not yet registered a port number, waiting for retry...", taskId);
                        Thread.Sleep(_retryDelay);
                    }
                } while( port == 0 );

                _taskConnections[taskIndex].HostName = taskServer.HostName;
                _taskConnections[taskIndex].Port = port;
            }

            _log.DebugFormat("Connecting to task {0} at TCP channel server {1}:{2}", _outputIds[taskIndex], _taskConnections[taskIndex].HostName, _taskConnections[taskIndex].Port);

            TcpClient client = new TcpClient(_taskConnections[taskIndex].HostName, _taskConnections[taskIndex].Port);
            if( _reuseConnections )
                client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            client.Client.NoDelay = true;
            return client;
        }
    }
}
