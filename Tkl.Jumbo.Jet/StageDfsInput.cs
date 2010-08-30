// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using System.Collections.ObjectModel;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides information about the DFS input to a stage.
    /// </summary>
    [XmlType(Namespace=JobConfiguration.XmlNamespace)]
    public sealed class StageDfsInput
    {
        private readonly ExtendedCollection<TaskDfsInput> _taskInputs = new ExtendedCollection<TaskDfsInput>();

        /// <summary>
        /// Gets the inputs for each task in the stage.
        /// </summary>
        /// <value>The task inputs.</value>
        public Collection<TaskDfsInput> TaskInputs
        {
            get { return _taskInputs; }
        }

        /// <summary>
        /// Gets or sets the type of the record reader to use to read the input data. This must be a type that inherits from <see cref="RecordReader{T}"/>.
        /// </summary>
        /// <value>The type of the record reader.</value>
        public TypeReference RecordReaderType { get; set; }


        /// <summary>
        /// Creates a record reader for the specified <see cref="TaskExecutionUtility"/>.
        /// </summary>
        /// <param name="taskExecution">The <see cref="TaskExecutionUtility"/> whose configuration to pass to the record reader. May be <see langword="null"/>.</param>
        /// <returns>
        /// A <see cref="RecordReader{T}"/> that reads the input specified in the <see cref="TaskDfsInput"/> for the task number of the <see cref="TaskExecutionUtility"/>.
        /// </returns>
        public IRecordReader CreateRecordReader(TaskExecutionUtility taskExecution)
        {
            return CreateRecordReader(taskExecution.DfsClient, taskExecution, taskExecution.Context.TaskAttemptId.TaskId.TaskNumber - 1);
        }

        /// <summary>
        /// Creates a record reader for the specified task number.
        /// </summary>
        /// <param name="dfsClient">The <see cref="DfsClient"/> to use to access the DFS.</param>
        /// <param name="inputIndex">The zero-based index of the input to use.</param>
        /// <returns>
        /// A <see cref="RecordReader{T}"/> that reads the input specified in the <see cref="TaskDfsInput"/> for the task number.
        /// </returns>
        public IRecordReader CreateRecordReader(DfsClient dfsClient, int inputIndex)
        {
            return CreateRecordReader(dfsClient, null, inputIndex);
        }

        private IRecordReader CreateRecordReader(DfsClient dfsClient, TaskExecutionUtility taskExecution, int inputIndex)
        {
            if( dfsClient == null )
                throw new ArgumentNullException("dfsClient");
            Type recordReaderType = RecordReaderType.ReferencedType;
            TaskDfsInput taskInput = _taskInputs[inputIndex];
            DfsInputStream inputStream = dfsClient.OpenFile(taskInput.Path);
            long offset;
            long size;
            long blockSize = inputStream.BlockSize;
            offset = blockSize * (long)taskInput.Block;
            size = Math.Min(blockSize, inputStream.Length - offset);
            return (IRecordReader)JetActivator.CreateInstance(recordReaderType, taskExecution, inputStream, offset, size, taskExecution == null ? false : taskExecution.AllowRecordReuse);
        }
    }
}
