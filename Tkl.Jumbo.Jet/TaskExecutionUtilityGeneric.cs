﻿using System;
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

            public override long OutputBytes
            {
                get
                {
                    if( _recordWriter == null )
                        return _bytesWritten;
                    else
                        return _bytesWritten + _recordWriter.OutputBytes;
                }
            }

            protected override void WriteRecordInternal(TOutput record)
            {
                _recordWriter.WriteRecord(record);
            }

            private void IMultiInputRecordReader_CurrentPartitionChanged(object sender, EventArgs e)
            {
                if( _recordWriter != null )
                {
                    _bytesWritten += _recordWriter.OutputBytes;
                    _recordWriter.Dispose();
                }

                CreateOutputWriter();
            }

            private void CreateOutputWriter()
            {

                _recordWriter = (RecordWriter<TOutput>)_task.CreateDfsOutputWriter(_reader.CurrentPartition);
            }

            protected override void Dispose(bool disposing)
            {
                base.Dispose(disposing);
                if( disposing )
                {
                    if( _recordWriter != null )
                    {
                        _bytesWritten += _recordWriter.OutputBytes;
                        _recordWriter.Dispose();
                        _recordWriter = null;
                    }
                }
            }
        }


        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskExecutionUtility)); // Intentionally not using the generic type as the log source.

        private bool _hasTaskRun;
        private PipelinePullTaskRecordWriter<TInput, TOutput> _pipelinePullTaskRecordWriter; // Needed to finish pipelined pull tasks.
        private PipelinePrepartitionedPushTaskRecordWriter<TInput, TOutput> _pipelinePrepartitionedPushTaskRecordWriter; // Needed to finish pipelined prepartitioned push tasks.
        private PrepartitionedRecordWriter<TOutput> _prepartitionedOutputWriter; // Needed to finish prepartitioned tasks.

        public TaskExecutionUtilityGeneric(DfsClient dfsClient, JetClient jetClient, ITaskServerUmbilicalProtocol umbilical, TaskExecutionUtility parentTask, TaskContext configuration)
            : base(dfsClient, jetClient, umbilical, parentTask, configuration)
        {
        }

        public override TaskMetrics RunTask()
        {
            CheckDisposed();
            if( IsAssociatedTask )
                throw new InvalidOperationException("You cannot run a child task.");
            if( _hasTaskRun )
                throw new InvalidOperationException("This task has already been run.");
            _hasTaskRun = true;

            RecordReader<TInput> input = (RecordReader<TInput>)InputReader;
            RecordWriter<TOutput> output = (RecordWriter<TOutput>)OutputWriter;
            Stopwatch taskStopwatch = new Stopwatch();

            // Ensure task object created and added to additional progress sources if needed before progress thread is started.
            ITask<TInput, TOutput> task = (ITask<TInput, TOutput>)Task;

            StartProgressThread();

            MultiInputRecordReader<TInput> multiInputReader = input as MultiInputRecordReader<TInput>;
            if( multiInputReader != null && InputChannels.Count == 1 && InputChannels[0].Configuration.PartitionsPerTask > 1 )
                RunTaskMultipleInputPartitions(multiInputReader, output, taskStopwatch, task);
            else
                CallTaskRunMethod(input, output, taskStopwatch, task);

            TimeSpan timeWaiting;
            MultiRecordReader<TInput> multiReader = input as MultiRecordReader<TInput>;
            if( multiReader != null )
                timeWaiting = multiReader.TimeWaiting;
            else
                timeWaiting = TimeSpan.Zero;
            _log.InfoFormat("Task finished execution, execution time: {0}s; time spent waiting for input: {1}s.", taskStopwatch.Elapsed.TotalSeconds, timeWaiting.TotalSeconds);

            TaskMetrics metrics = new TaskMetrics();
            FinalizeTask(metrics);

            metrics.LogMetrics();

            return metrics;
        }

        protected override IRecordWriter CreateOutputRecordWriter()
        {
            if( Context.StageConfiguration.DfsOutput != null )
            {
                if( Context.StageConfiguration.InternalPartitionCount == 1 )
                {
                    if( !ProcessesAllInputPartitions && RootTask.InputChannels != null && RootTask.InputChannels.Count == 1 && RootTask.InputChannels[0].Configuration.PartitionsPerTask > 1 )
                        return new PartitionDfsOutputRecordWriter(this);
                }
                return (RecordWriter<TOutput>)CreateDfsOutputWriter(Context.TaskId.TaskNumber);
            }
            else if( OutputChannel != null )
            {
                //_log.Debug("Creating output channel record writer.");
                return OutputChannel.CreateRecordWriter<TOutput>();
            }
            else
                return null;
        }

        internal override IRecordWriter CreatePipelineRecordWriter(object partitioner)
        {
            if( !IsAssociatedTask )
                throw new InvalidOperationException("Can't create pipeline record writer for non-child task.");

            RecordWriter<TOutput> output = (RecordWriter<TOutput>)OutputWriter;

            object task = Task;
            IPushTask<TInput, TOutput> pushTask = task as IPushTask<TInput, TOutput>;
            if( pushTask != null )
                return new PipelinePushTaskRecordWriter<TInput, TOutput>(this, output);
            else
            {
                IPrepartitionedPushTask<TInput, TOutput> prepartitionedPushTask = task as IPrepartitionedPushTask<TInput, TOutput>;
                if( prepartitionedPushTask != null )
                {
                    IPartitioner<TInput> partitioner2 = (IPartitioner<TInput>)partitioner;
                    partitioner2.Partitions = Context.StageConfiguration.InternalPartitionCount;
                    _pipelinePrepartitionedPushTaskRecordWriter = new PipelinePrepartitionedPushTaskRecordWriter<TInput, TOutput>(this, output, partitioner2);
                    return _pipelinePrepartitionedPushTaskRecordWriter;
                }
                else
                {
                    _pipelinePullTaskRecordWriter = new PipelinePullTaskRecordWriter<TInput, TOutput>(this, output, Context.TaskId);
                    return _pipelinePullTaskRecordWriter;
                }
            }
        }

        private void CallTaskRunMethod(RecordReader<TInput> input, RecordWriter<TOutput> output, Stopwatch taskStopwatch, ITask<TInput, TOutput> task)
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
                IPushTask<TInput, TOutput> pushTask = task as IPushTask<TInput, TOutput>;
                if( pushTask != null )
                {
                    _log.Info("Running push task.");
                    taskStopwatch.Start();
                    foreach( TInput record in input.EnumerateRecords() )
                    {
                        pushTask.ProcessRecord(record, output);
                    }
                    // Finish is called by taskExecution.FinishTask below.
                    taskStopwatch.Stop();
                }
                else
                {
                    IPrepartitionedPushTask<TInput, TOutput> prepartitionedPushTask = (IPrepartitionedPushTask<TInput, TOutput>)task;
                    PrepartitionedRecordWriter<TOutput> prepartitionedOutputWriter = new PrepartitionedRecordWriter<TOutput>(output);
                    _prepartitionedOutputWriter = prepartitionedOutputWriter;
                    // If a prepartitioned push task is the root of a stage, we will assign all records to partition 0. The task doesn't get to know about multiple partitions per task, because that's not what its for.
                    _log.Info("Running prepartitioned push task.");
                    taskStopwatch.Start();
                    foreach( TInput record in input.EnumerateRecords() )
                    {
                        prepartitionedPushTask.ProcessRecord(record, 0, prepartitionedOutputWriter);
                    }
                }
            }

            FinishTask();
        }

        protected override void RunTaskFinishMethod(bool isFinalizing)
        {
            // For a task that processes all input partitions, we don't call finish until we're finalizing.
            // For a task that doesn't, we call finish unless we're finalizing.
            if( isFinalizing == ProcessesAllInputPartitions )
            {
                IPushTask<TInput, TOutput> task = Task as IPushTask<TInput, TOutput>;
                if( task != null )
                    task.Finish((RecordWriter<TOutput>)OutputWriter);
                else if( _pipelinePrepartitionedPushTaskRecordWriter != null )
                    _pipelinePrepartitionedPushTaskRecordWriter.Finish();
                else if( _prepartitionedOutputWriter != null )
                    ((IPrepartitionedPushTask<TInput, TOutput>)Task).Finish(_prepartitionedOutputWriter);
                else if( _pipelinePullTaskRecordWriter != null )
                    _pipelinePullTaskRecordWriter.Finish();
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if( disposing )
            {
                if( _prepartitionedOutputWriter != null )
                {
                    _prepartitionedOutputWriter.Dispose();
                    _prepartitionedOutputWriter = null;
                }
            }
        }

        private void RunTaskMultipleInputPartitions(MultiInputRecordReader<TInput> input, RecordWriter<TOutput> output, Stopwatch taskStopwatch, ITask<TInput, TOutput> task)
        {
            IPullTask<TInput, TOutput> pullTask;
            if( ProcessesAllInputPartitions && (pullTask = task as IPullTask<TInput, TOutput>) != null )
            {
                using( MultiPartitionRecordReader<TInput> partitionReader = new MultiPartitionRecordReader<TInput>(this, input) )
                {
                    _log.Info("Running pull task.");
                    taskStopwatch.Start();
                    pullTask.Run(partitionReader, output);
                    taskStopwatch.Stop();
                }
            }
            else
            {
                input.CurrentPartitionChanging += new EventHandler<CurrentPartitionChangingEventArgs>(input_CurrentPartitionChanging);
                TotalInputPartitions = input.PartitionCount;
                bool firstPartition = true;
                _log.Info("Running push task.");
                do
                {
                    _log.InfoFormat("Running task for partition {0}.", input.CurrentPartition);
                    if( firstPartition )
                        firstPartition = false;
                    else
                    {
                        ResetForNextPartition();
                        task = (ITask<TInput, TOutput>)Task;
                    }
                    CallTaskRunMethod(input, output, taskStopwatch, task);
                    _log.InfoFormat("Finished running task for partition {0}.", input.CurrentPartition);
                    // If input.NextPartition fails we will check for additional partitions, and if we got any, we need to call input.NextPartition again.
                } while( input.NextPartition() || (GetAdditionalPartitions(input) && input.NextPartition()) );
            }
        }

        private void input_CurrentPartitionChanging(object sender, CurrentPartitionChangingEventArgs e)
        {
            e.Cancel = !NotifyStartPartitionProcessing(e.NewPartitionNumber);
        }
    }
}
