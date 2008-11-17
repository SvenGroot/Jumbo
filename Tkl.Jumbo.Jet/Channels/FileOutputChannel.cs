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
        /// <summary>
        /// Initializes a new instance of the <see cref="FileOutputChannel"/> class.
        /// </summary>
        /// <param name="jobDirectory">The directory on the local file system where files related to this job are stored.</param>
        /// <param name="channelConfig">The <see cref="ChannelConfiguration"/> this channel.</param>
        public FileOutputChannel(string jobDirectory, ChannelConfiguration channelConfig)
        {
            if( jobDirectory == null )
                throw new ArgumentNullException("jobDirectory");
            if( channelConfig == null )
                throw new ArgumentNullException("channelConfig");

            FileName = Path.Combine(jobDirectory, string.Format("{0}_{1}.output", channelConfig.InputTaskID, channelConfig.OutputTaskID));
        }

        /// <summary>
        /// Gets the path of the file where the channel's output is stored.
        /// </summary>
        public string FileName { get; private set; }

        #region IOutputChannel members

        /// <summary>
        /// Creates a <see cref="RecordWriter{T}"/> to which the channel can write its output.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="RecordWriter{T}"/> for the channel.</returns>
        public RecordWriter<T> CreateRecordWriter<T>() where T : IWritable, new()
        {
            // The RecordWriter will dispose the stream, so we don't need to worry about it.
            return new BinaryRecordWriter<T>(File.Create(FileName));
        }

        #endregion
    }
}
