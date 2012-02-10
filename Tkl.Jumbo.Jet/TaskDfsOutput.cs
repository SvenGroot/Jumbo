// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.IO;
using System.IO;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides information about output that a task will write to the Distributed File System.
    /// </summary>
    [XmlType(Namespace=JobConfiguration.XmlNamespace)]
    public class TaskDfsOutput : ICloneable
    {
        /// <summary>
        /// Gets or sets the path of the file to write.
        /// </summary>
        [XmlAttribute("path")]
        public string PathFormat { get; set; }

        /// <summary>
        /// Gets or sets the type of <see cref="Tkl.Jumbo.IO.RecordWriter{T}"/> to use to write the file.
        /// </summary>
        public TypeReference RecordWriterType { get; set; }

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
        /// Gets or sets the record options of the output file.
        /// </summary>
        /// <value>The record options.</value>
        [XmlAttribute("recordOptions")]
        public RecordStreamOptions RecordOptions { get; set; }

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
            if( taskExecution == null )
                throw new ArgumentNullException("taskExecution");
            // It's the record writer's job to dispose the stream.
            Stream outputStream = taskExecution.FileSystemClient.CreateFile(fileName, BlockSize, ReplicationFactor, RecordOptions);
            //_log.DebugFormat("Creating record writer of type {0}", Configuration.StageConfiguration.DfsOutput.RecordWriterTypeName);
            return (IRecordWriter)JetActivator.CreateInstance(RecordWriterType.ReferencedType, taskExecution, outputStream);
        }

        #region ICloneable Members

        object ICloneable.Clone()
        {
            return Clone();
        }

        #endregion
    }
}
