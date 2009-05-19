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
        private string _recordReaderTypeName;
        private Type _recordReaderType;

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
        /// Gets or sets the name of the type of <see cref="Tkl.Jumbo.IO.RecordReader{T}"/> to use to read the file.
        /// </summary>
        [XmlAttribute("recordReader")]
        public string RecordReaderTypeName
        {
            get { return _recordReaderTypeName; }
            set
            {
                _recordReaderTypeName = value;
                _recordReaderType = null;
            }
        }

        /// <summary>
        /// Gets or sets the type of <see cref="Tkl.Jumbo.IO.RecordReader{T}"/> to use to read the file.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   Record readers must inherit from <see cref="Tkl.Jumbo.IO.RecordReader{T}"/>.
        /// </para>
        /// <para>
        ///   A record reader can be used for a <see cref="TaskDfsInput"/> if it provides
        ///   a constructor that takes arguments to specify a <see cref="System.IO.Stream"/> to read
        ///   from, an <see cref="Int64"/> specifying the offset to start reading in the stream,
        ///   an <see cref="Int64"/> specifying the number of bytes to read from the stream,
        ///   and a <see cref="Boolean"/> that indicates whether record reuse is allowed.
        /// </para>
        /// <para>
        ///   In addition, record readers for a <see cref="TaskDfsInput"/> must be able
        ///   to find the start of the next record from the specified offset. They are
        ///   allowed to read more than the specified number of bytes if the end of the
        ///   region is not on a record boundary. If record reader A reads offset X
        ///   and size Y, and record reader B reads from offset X + Y, the implementation
        ///   must take care that no records are read by both readers.
        /// </para>
        /// </remarks>
        [XmlIgnore]
        public Type RecordReaderType
        {
            get
            {
                if( _recordReaderType == null && _recordReaderTypeName != null )
                    _recordReaderType = Type.GetType(_recordReaderTypeName, true);
                return _recordReaderType;
            }
            set
            {
                _recordReaderType = value;
                _recordReaderTypeName = value == null ? null : value.AssemblyQualifiedName;
            }
        }

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
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public static RecordReader<T> CreateRecordReader<T>(this TaskDfsInput input, DfsClient dfsClient, TaskExecutionUtility taskExecution)
            where T : IWritable, new()
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( dfsClient == null )
                throw new ArgumentNullException("dfsClient");
            Type recordReaderType = input.RecordReaderType;
            long offset;
            long size;
            long blockSize = dfsClient.NameServer.BlockSize;
            offset = blockSize * (long)input.Block;
            size = Math.Min(blockSize, dfsClient.NameServer.GetFileInfo(input.Path).Size - offset);
            DfsInputStream inputStream = dfsClient.OpenFile(input.Path);
            return (RecordReader<T>)JetActivator.CreateInstance(recordReaderType, taskExecution, inputStream, offset, size, taskExecution.AllowRecordReuse);
        }
    }
}
