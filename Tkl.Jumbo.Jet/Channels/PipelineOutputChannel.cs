// $Id$
//
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
    public sealed class PipelineOutputChannel : IOutputChannel
    {
        #region Nested types

        internal sealed class PipelineRecordWriter<TRecord, TPipelinedTaskOutput> : RecordWriter<TRecord>
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
                        if( _output != null )
                        {
                            _output.Dispose();
                            _output = null;
                        }
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
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public RecordWriter<T> CreateRecordWriter<T>()
        {
            StageConfiguration childStage = _taskExecution.Configuration.StageConfiguration.ChildStage;
            if( childStage.TaskCount == 1 )
                return (RecordWriter<T>)_taskExecution.CreateAssociatedTask(childStage, 1).CreatePipelineRecordWriter();
            else
            {
                List<RecordWriter<T>> writers = new List<RecordWriter<T>>();
                IPartitioner<T> partitioner = (IPartitioner<T>)JetActivator.CreateInstance(_taskExecution.Configuration.StageConfiguration.ChildStagePartitionerType.ReferencedType, _taskExecution);

                for( int x = 1; x <= childStage.TaskCount; ++x )
                {
                    TaskExecutionUtility childTaskExecution = _taskExecution.CreateAssociatedTask(childStage, x);
                    writers.Add((RecordWriter<T>)childTaskExecution.CreatePipelineRecordWriter());
                }
                return new MultiRecordWriter<T>(writers, partitioner);
            }
        }

        #endregion

    }
}
