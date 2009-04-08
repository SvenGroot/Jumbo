using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Reflection;
using System.IO;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Represents the the channel between two pipelined tasks.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   Pipelined tasks are run in the same process, and each call to <see cref="RecordWriter{T}.WriteRecord"/> will invoke
    ///   the associated task's <see cref="IPushTask{TInput,TOutput}.ProcessRecord"/> method. Because of this, there is no
    ///   associated input channel for this channel type.
    /// </para>
    /// </remarks>
    public class PipelineOutputChannel : IOutputChannel
    {
        #region Nested types

        private class PipelineRecordWriter<TRecord, TPipelinedTaskOutput> : RecordWriter<TRecord>
            where TRecord : IWritable, new()
            where TPipelinedTaskOutput : IWritable, new()
        {
            private IPushTask<TRecord, TPipelinedTaskOutput> _task;
            private RecordWriter<TPipelinedTaskOutput> _output;

            public PipelineRecordWriter(IPushTask<TRecord, TPipelinedTaskOutput> task, RecordWriter<TPipelinedTaskOutput> output)
            {
                if( task == null )
                    throw new ArgumentNullException("task");
                if( output == null )
                    throw new ArgumentNullException("output");

                _task = task;
                _output = output;
            }

            protected override void WriteRecordInternal(TRecord record)
            {
                if( _task == null )
                    throw new ObjectDisposedException(typeof(PipelineRecordWriter<TRecord, TPipelinedTaskOutput>).FullName);
                _task.ProcessRecord(record, _output);
            }

            protected override void Dispose(bool disposing)
            {
                try
                {
                    if( disposing )
                    {
                        _task.Finish(_output);
                        _task = null;
                        _output.Dispose();
                        _output = null;
                    }
                }
                finally
                {
                    base.Dispose(disposing);
                }
            }
        }

        #endregion

        private TaskExecutionUtility _taskExecution;

        /// <summary>
        /// Initializes a new instance of the <see cref="PipelineOutputChannel"/> class.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
        public PipelineOutputChannel(TaskExecutionUtility taskExecution)
        {
            if( taskExecution == null )
                throw new ArgumentNullException("taskExecution");

            _taskExecution = taskExecution;
        }

        #region IOutputChannel Members

        /// <summary>
        /// Creates a record writer for the channel.
        /// </summary>
        /// <typeparam name="T">The type of record.</typeparam>
        /// <returns>A record writer for the channel.</returns>
        public Tkl.Jumbo.IO.RecordWriter<T> CreateRecordWriter<T>() where T : Tkl.Jumbo.IO.IWritable, new()
        {
            if( _taskExecution.OutputChannelConfiguration.OutputTasks.Length == 1 )
                return CreateRecordWriter<T>(_taskExecution.CreateAssociatedTask(_taskExecution.OutputChannelConfiguration.OutputTasks[0]));
            else
            {
                IPartitioner<T> partitioner = (IPartitioner<T>)Activator.CreateInstance(Type.GetType(_taskExecution.OutputChannelConfiguration.PartitionerType));
                var writers = from taskId in _taskExecution.OutputChannelConfiguration.OutputTasks
                              let taskExecution = _taskExecution.CreateAssociatedTask(taskId)
                              select CreateRecordWriter<T>(taskExecution);
                return new MultiRecordWriter<T>(writers, partitioner);
            }
        }

        #endregion

        private RecordWriter<T> CreateRecordWriter<T>(TaskExecutionUtility pipelinedTask) where T : IWritable, new()
        {
            MethodInfo createWriterMethod = typeof(PipelineOutputChannel)
                                                .GetMethod("CreateRecordWriter", BindingFlags.NonPublic | BindingFlags.Static)
                                                .MakeGenericMethod(typeof(T), pipelinedTask.OutputRecordType);
            return (RecordWriter<T>)createWriterMethod.Invoke(this, new object[] { pipelinedTask });
        }

        private RecordWriter<TRecord> CreateRecordWriter<TRecord, TPipelinedTaskOutput>(TaskExecutionUtility pipelinedTask)
            where TRecord : IWritable, new()
            where TPipelinedTaskOutput : IWritable, new()
        {
            RecordWriter<TPipelinedTaskOutput> output = pipelinedTask.GetOutputWriter<TPipelinedTaskOutput>();

            IPushTask<TRecord, TPipelinedTaskOutput> task = (IPushTask<TRecord, TPipelinedTaskOutput>)pipelinedTask.InstantiateTask<TRecord, TPipelinedTaskOutput>();

            return new PipelineRecordWriter<TRecord, TPipelinedTaskOutput>(task, output);
        }
    }
}
