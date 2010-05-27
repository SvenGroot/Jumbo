// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides information about output that a task will write to the Distributed File System.
    /// </summary>
    [XmlType(Namespace=JobConfiguration.XmlNamespace)]
    public class TaskDfsOutput : ICloneable
    {
        private string _recordWriterTypeName;
        private Type _recordWriterType;

        /// <summary>
        /// Gets or sets the path of the file to write.
        /// </summary>
        [XmlAttribute("path")]
        public string PathFormat { get; set; }

        /// <summary>
        /// Gets or sets the name of the type of <see cref="Tkl.Jumbo.IO.RecordWriter{T}"/> to use to write the file.
        /// </summary>
        [XmlAttribute("recordWriter")]
        public string RecordWriterTypeName
        {
            get { return _recordWriterTypeName; }
            set 
            {
                _recordWriterTypeName = value;
                _recordWriterType = null;
            }
        }

        /// <summary>
        /// Gets or sets the type of <see cref="Tkl.Jumbo.IO.RecordWriter{T}"/> to use to write the file.
        /// </summary>
        [XmlIgnore]
        public Type RecordWriterType
        {
            get 
            {
                if( _recordWriterType == null && _recordWriterTypeName != null )
                    _recordWriterType = Type.GetType(_recordWriterTypeName, true);
                return _recordWriterType; 
            }
            set 
            {
                _recordWriterType = value;
                _recordWriterTypeName = value == null ? null : value.AssemblyQualifiedName;
            }
        }

        /// <summary>
        /// Gets or sets the block size of the output file, or zero to use the file system default block size.
        /// </summary>
        [XmlAttribute("blockSize")]
        public int BlockSize { get; set; }

        /// <summary>
        /// Gets or sets the replication factor of the output file, or zero to use the file system default replication factor.
        /// </summary>
        [XmlAttribute("replicationFactor")]
        public int ReplicationFactor { get; set; }

        /// <summary>
        /// Creates a clone of the current object.
        /// </summary>
        /// <returns>A clone of the current object.</returns>
        public TaskDfsOutput Clone()
        {
            return (TaskDfsOutput)MemberwiseClone();
        }

        /// <summary>
        /// Gets the output path for the specified task number.
        /// </summary>
        /// <param name="taskNumber">The task number.</param>
        /// <returns>The output path.</returns>
        public string GetPath(int taskNumber)
        {
            return string.Format(System.Globalization.CultureInfo.InvariantCulture, PathFormat, taskNumber);
        }


        /// <summary>
        /// Creates the DFS output record writer.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task.</param>
        /// <param name="fileName">Name of the file.</param>
        /// <returns>A record writer of the type specified in <see cref="RecordWriterType"/></returns>
        public IRecordWriter CreateRecordWriter(TaskExecutionUtility taskExecution, string fileName)
        {
            // It's the record writer's job to dispose the stream.
            DfsOutputStream outputStream = taskExecution.DfsClient.CreateFile(fileName, BlockSize, ReplicationFactor);
            //_log.DebugFormat("Creating record writer of type {0}", Configuration.StageConfiguration.DfsOutput.RecordWriterTypeName);
            return (IRecordWriter)JetActivator.CreateInstance(RecordWriterType, taskExecution, outputStream);
        }

        #region ICloneable Members

        object ICloneable.Clone()
        {
            return Clone();
        }

        #endregion
    }
}
