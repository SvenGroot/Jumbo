using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides information about the input that a task will read from the distributed file system.
    /// </summary>
    [XmlType(Namespace=JobConfiguration.XmlNamespace)]
    public class TaskDfsInput : ICloneable
    {
        /// <summary>
        /// Gets or sets the path of the file to read.
        /// </summary>
        [XmlAttribute("path")]
        public string Path { get; set; }

        /// <summary>
        /// Gets or sets the zero-based index of the block the file wants to read.
        /// </summary>
        [XmlAttribute("block")]
        public int Block { get; set; }

        /// <summary>
        /// Gets or sets the type of <see cref="Tkl.Jumbo.IO.RecordReader{T}"/> to use to read the file.
        /// </summary>
        [XmlAttribute("recordReaderType")]
        public string RecordReaderType { get; set; }

        /// <summary>
        /// Creates a clone of the current object.
        /// </summary>
        /// <returns>A clone of the current object.</returns>
        public TaskDfsInput Clone()
        {
            return (TaskDfsInput)MemberwiseClone();
        }

        #region ICloneable Members

        object ICloneable.Clone()
        {
            return Clone();
        }

        #endregion
    }

    /// <summary>
    /// Extension methods for <see cref="TaskDfsInput"/>.
    /// </summary>
    public static class TaskDfsInputExtensions
    {
        /// <summary>
        /// Creates a record reader for the specified <see cref="TaskDfsInput"/>.
        /// </summary>
        /// <param name="input">The <see cref="TaskDfsInput"/> for which to create a record reader.</param>
        /// <param name="dfsClient">The <see cref="DfsClient"/> to use to access the DFS.</param>
        /// <param name="taskExecution">The <see cref="TaskExecutionUtility"/> whose configuration to pass to the record reader. May be <see langword="null"/>.</param>
        /// <returns>A <see cref="RecordReader{T}"/> that reads the input specified in the <see cref="TaskDfsInput"/>.</returns>
        /// <remarks>
        /// This is done as an extension because XML serialization doesn't like it if this method is on the actual class.
        /// </remarks>
        public static RecordReader<T> CreateRecordReader<T>(this TaskDfsInput input, DfsClient dfsClient, TaskExecutionUtility taskExecution)
            where T : IWritable, new()
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( dfsClient == null )
                throw new ArgumentNullException("dfsClient");
            Type recordReaderType = Type.GetType(input.RecordReaderType);
            long offset;
            long size;
            long blockSize = dfsClient.NameServer.BlockSize;
            offset = blockSize * (long)input.Block;
            size = Math.Min(blockSize, dfsClient.NameServer.GetFileInfo(input.Path).Size - offset);
            DfsInputStream inputStream = dfsClient.OpenFile(input.Path);
            return (RecordReader<T>)JetActivator.CreateInstance(recordReaderType, taskExecution, inputStream, offset, size);
        }
    }
}
