﻿// $Id$
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
        private int _splitsPerBlock = 1;

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
        /// Gets or sets the number of tasks per input.
        /// </summary>
        /// <value>The number of tasks per input.</value>
        [XmlAttribute("splitsPerBlock")]
        public int SplitsPerBlock
        {
            get { return _splitsPerBlock; }
            set 
            {
                if( value < 1 )
                    throw new ArgumentOutOfRangeException("value");
                _splitsPerBlock = value; 
            }
        }

        /// <summary>
        /// Gets the number of input splits.
        /// </summary>
        /// <value>The input split count.</value>
        [XmlIgnore]
        public int SplitCount
        {
            get { return TaskInputs.Count * SplitsPerBlock; }
        }

        /// <summary>
        /// Gets the index of the input for the specified split.
        /// </summary>
        /// <param name="splitIndex">Zero-based index of the split.</param>
        /// <returns>The zero-based input index.</returns>
        public int GetInputIndex(int splitIndex)
        {
            int inputIndex = splitIndex / SplitsPerBlock;
            if( inputIndex < 0 || inputIndex >= _taskInputs.Count )
                throw new ArgumentOutOfRangeException("splitIndex");
            return inputIndex;
        }

        /// <summary>
        /// Gets the index of the input for the specified task.
        /// </summary>
        /// <param name="taskId">The task id.</param>
        /// <returns>The zero-based input index.</returns>
        public int GetInputIndex(TaskId taskId)
        {
            if( taskId == null )
                throw new ArgumentNullException("taskId");
            return GetInputIndex(taskId.TaskNumber - 1);
        }

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
        /// <param name="splitIndex">The zero-based index of the input to use.</param>
        /// <returns>
        /// A <see cref="RecordReader{T}"/> that reads the input specified in the <see cref="TaskDfsInput"/> for the task number.
        /// </returns>
        public IRecordReader CreateRecordReader(DfsClient dfsClient, int splitIndex)
        {
            return CreateRecordReader(dfsClient, null, splitIndex);
        }

        private IRecordReader CreateRecordReader(DfsClient dfsClient, TaskExecutionUtility taskExecution, int splitIndex)
        {
            if( dfsClient == null )
                throw new ArgumentNullException("dfsClient");
            Type recordReaderType = RecordReaderType.ReferencedType;
            int inputIndex = splitIndex / SplitsPerBlock;
            int blockSplitIndex = splitIndex % SplitsPerBlock;
            TaskDfsInput taskInput = _taskInputs[inputIndex];
            DfsInputStream inputStream = dfsClient.OpenFile(taskInput.Path);
            long blockSize = inputStream.BlockSize;
            long offset = blockSize * (long)taskInput.Block;
            blockSize = Math.Min(blockSize, inputStream.Length - offset);
            long splitSize = (blockSize / SplitsPerBlock);
            offset += (splitSize * blockSplitIndex);
            if( blockSplitIndex == SplitsPerBlock )
                splitSize = blockSize - offset;

            return (IRecordReader)JetActivator.CreateInstance(recordReaderType, taskExecution, inputStream, offset, splitSize, taskExecution == null ? false : taskExecution.AllowRecordReuse);
        }
    }
}
