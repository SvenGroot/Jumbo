using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.IO;
using System.Diagnostics;
using Tkl.Jumbo.Jet.Channels;

namespace Tkl.Jumbo.Jet
{
    sealed class TaskExecutionUtilityGeneric<TInput, TOutput> : TaskExecutionUtility
    {
        #region Nested types

        // This class is used if the input of a compound task is a channel and the output is a file (and there is no internal partitioning)
        // in which case we want to name output files after partitions rather than task numbers. Since there can be more than one partition,
        // this writer keeps an eye on 
        private sealed class PartitionDfsOutputRecordWriter : RecordWriter<TOutput>
        {
            private readonly TaskExecutionUtility _task;
            private readonly TaskExecutionUtility _rootTask;
            private RecordWriter<TOutput> _recordWriter;
            private IMultiInputRecordReader _reader;
            private long _bytesWritten;

            public PartitionDfsOutputRecordWriter(TaskExecutionUtility task)
            {
                _task = task;
                _rootTask = task.RootTask;
                
                _reader = (IMultiInputRecordReader)_rootTask.InputReader;
                _reader.CurrentPartitionChanged += new EventHandler(IMultiInputRecordReader_CurrentPartitionChanged);
                CreateOutputWriter();
            }

            public override long BytesWritten
            {
                get
                {
                    if( _recordWriter == null )
                        return _bytesWritten;
                    else
                        return _bytesWritten + _recordWriter.BytesWritten;
                }
            }

            protected override void WriteRecordInternal(TOutput record)
            {
                _recordWriter.WriteRecord(record);
            }

            private void IMultiInputRecordReader_CurrentPartitionChanged(object sender, EventArgs e)
            {
                CreateOutputWriter();
            }

            private void CreateOutputWriter()
            {
                if( _recordWriter != null )
                {
                    _bytesWritten += _recordWriter.BytesWritten;
                    _recordWriter.Dispose();
                }

                _recordWriter = (RecordWriter<TOutput>)_task.CreateDfsOutputWriter(_reader.CurrentPartition);
            }

            protected override void Dispose(bool disposing)
            {
                base.Dispose(disposing);
                if( disposing )
                {
                    if( _recordWriter != null )
                    {
                        _bytesWritten += _recordWriter.BytesWritten;
                        _recordWriter.Dispose();
                        _recordWriter = null;
                    }
                }
            }
        }


        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskExecutionUtility)); // Intentionally not using the generic type as the log source.

        private bool _hasTaskRun;

        public TaskExecutionUtilityGeneric(DfsClient dfsClient, JetClient jetClient, ITaskServerUmbilicalProtocol umbilical, TaskExecutionUtility parentTask, TaskAttemptConfiguration configuration)
            : base(dfsClient, jetClient, umbilical, parentTask, configuration)
        {
        }

        public override void RunTask()
        {
            CheckDisposed();
            if( IsAssociatedTask )
                throw new InvalidOperationException("You cannot run a child task.");
            if( _hasTaskRun )
                throw new InvalidOperationException("This task has already been run.");
            _hasTaskRun = true;

            ITask<TInput, TOutput> task = (ITask<TInput, TOutput>)Task;
            RecordReader<TInput> input = (RecordReader<TInput>)InputReader;
            RecordWriter<TOutput> output = (RecordWriter<TOutput>)OutputWriter;
            Stopwatch taskStopwatch = new Stopwatch();

            StartProgressThread();

            IMultiInputRecordReader multiInputReader = input as IMultiInputRecordReader;
            if( multiInputReader != null )
            {
                foreach( int partition in multiInputReader.Partitions )
                {
                    _log.InfoFormat("Running task for partition {0}.", partition);
                    multiInputReader.CurrentPartition = partition;
                    CallTaskRunMethod(input, output, taskStopwatch, task);
                    _log.InfoFormat("Finished running task for partition {0}.", partition);
                }
            }
            else
                CallTaskRunMethod(input, output, taskStopwatch, task);
            TimeSpan timeWaiting;

            MultiRecordReader<TInput> multiReader = input as MultiRecordReader<TInput>;
            if( multiReader != null )
                timeWaiting = multiReader.TimeWaiting;
            else
                timeWaiting = TimeSpan.Zero;
            _log.InfoFormat("Task finished execution, execution time: {0}s; time spent waiting for input: {1}s.", taskStopwatch.Elapsed.TotalSeconds, timeWaiting.TotalSeconds);

            FinishTask();

            // TODO: Proper metrics for pipelined tasks.
            TaskMetrics metrics = CalculateMetrics();
            _log.Info(metrics);
        }

        protected override IRecordWriter CreateOutputRecordWriter()
        {
            if( Configuration.StageConfiguration.DfsOutput != null )
            {
                if( Configuration.StageConfiguration.InternalPartitionCount == 1 )
                {
                    if( RootTask.InputChannels != null && RootTask.InputChannels.Count == 1 )
                        return new PartitionDfsOutputRecordWriter(this);
                }
                return (RecordWriter<TOutput>)CreateDfsOutputWriter(Configuration.TaskId.TaskNumber);
            }
            else if( OutputChannel != null )
            {
                //_log.Debug("Creating output channel record writer.");
                return OutputChannel.CreateRecordWriter<TOutput>();
            }
            else
                return null;
        }

        internal override IRecordWriter CreatePipelineRecordWriter()
        {
            if( !IsAssociatedTask )
                throw new InvalidOperationException("Can't create pipeline record writer for non-child task.");

            RecordWriter<TOutput> output = (RecordWriter<TOutput>)OutputWriter;

            IPushTask<TInput, TOutput> task = (IPushTask<TInput, TOutput>)Task;

            return new PipelineOutputChannel.PipelineRecordWriter<TInput, TOutput>(task, output);            
        }

        private static void CallTaskRunMethod(RecordReader<TInput> input, RecordWriter<TOutput> output, Stopwatch taskStopwatch, ITask<TInput, TOutput> task)
        {
            IPullTask<TInput, TOutput> pullTask = task as IPullTask<TInput, TOutput>;
            if( pullTask != null )
            {
                _log.Info("Running pull task.");
                taskStopwatch.Start();
                pullTask.Run(input, output);
                taskStopwatch.Stop();
            }
            else
            {
                IPushTask<TInput, TOutput> pushTask = (IPushTask<TInput, TOutput>)task;
                _log.Info("Running push task.");
                taskStopwatch.Start();
                foreach( TInput record in input.EnumerateRecords() )
                {
                    pushTask.ProcessRecord(record, output);
                }
                // Finish is called by taskExecution.FinishTask below.
                taskStopwatch.Stop();
            }
        }

        protected override void RunTaskFinishMethod()
        {
            IPushTask<TInput, TOutput> task = Task as IPushTask<TInput, TOutput>;
            if( task != null )
                task.Finish((RecordWriter<TOutput>)OutputWriter);
        }
    }
}
