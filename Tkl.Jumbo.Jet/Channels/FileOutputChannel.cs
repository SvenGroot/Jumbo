using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Represents the writing end of a file channel between two tasks.
    /// </summary>
    public class FileOutputChannel : IOutputChannel
    {
        private readonly string[] _fileNames;
        private string _partitionerType;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileOutputChannel"/> class.
        /// </summary>
        /// <param name="jobDirectory">The directory on the local file system where files related to this job are stored.</param>
        /// <param name="channelConfig">The <see cref="ChannelConfiguration"/> this channel.</param>
        /// <param name="inputTaskId">The name of the task for which this channel is created. This should be one of the
        /// task IDs listed in the <see cref="ChannelConfiguration.InputTasks"/> property of the <paramref name="channelConfig"/>
        /// parameter.</param>
        public FileOutputChannel(string jobDirectory, ChannelConfiguration channelConfig, string inputTaskId)
        {
            if( jobDirectory == null )
                throw new ArgumentNullException("jobDirectory");
            if( channelConfig == null )
                throw new ArgumentNullException("channelConfig");

            _fileNames = (from outputTaskId in channelConfig.OutputTasks
                          select Path.Combine(jobDirectory, CreateChannelFileName(inputTaskId, outputTaskId))).ToArray();
            _partitionerType = channelConfig.PartitionerType;
        }


        internal static string CreateChannelFileName(string inputTaskID, string outputTaskID)
        {
            return string.Format("{0}_{1}.output", inputTaskID, outputTaskID);
        }

        #region IOutputChannel members

        /// <summary>
        /// Creates a <see cref="RecordWriter{T}"/> to which the channel can write its output.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="RecordWriter{T}"/> for the channel.</returns>
        public RecordWriter<T> CreateRecordWriter<T>() where T : IWritable, new()
        {
            if( _fileNames.Length == 1 )
                return new BinaryRecordWriter<T>(File.Create(_fileNames[0]));
            else
            {
                IPartitioner<T> partitioner = (IPartitioner<T>)Activator.CreateInstance(Type.GetType(_partitionerType));
                var writers = from file in _fileNames
                              select (RecordWriter<T>)new BinaryRecordWriter<T>(File.Create(file));
                return new MultiRecordWriter<T>(writers, partitioner);
            }
        }

        #endregion
    }
}
