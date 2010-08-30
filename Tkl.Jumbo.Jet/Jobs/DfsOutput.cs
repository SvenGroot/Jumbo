// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Represents output written to the DFS for a stage in a job being built by the <see cref="JobBuilder"/> class.
    /// </summary>
    public sealed class DfsOutput : IStageOutput
    {
        private readonly string _path;
        private Type _recordWriterType;
        private Type _recordType;

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsOutput"/> class.
        /// </summary>
        /// <param name="path">The path of a directory on the DFS to write the output to.</param>
        /// <param name="recordWriterType">Type of the record writer to use. This must be a type inheriting from <see cref="RecordWriter{T}"/>.</param>
        public DfsOutput(string path, Type recordWriterType)
        {
            if( path == null )
                throw new ArgumentNullException("path");
            if( recordWriterType == null )
                throw new ArgumentNullException("recordWriterType");

            Type recordWriterBaseType = recordWriterType.FindGenericBaseType(typeof(RecordWriter<>), true);

            _path = path;
            _recordWriterType = recordWriterType;
            if( !_recordWriterType.IsGenericTypeDefinition )
                _recordType = recordWriterBaseType.GetGenericArguments()[0];
        }

        /// <summary>
        /// Gets the path of a directory on the DFS that the output is written to.
        /// </summary>
        /// <value>The path of a directory on the DFS.</value>
        public string Path
        {
            get { return _path; }
        }

        /// <summary>
        /// Gets the type of the record writer.
        /// </summary>
        /// <value>The <see cref="Type"/> instance for the record writer. This will be a type inheriting from <see cref="RecordWriter{T}"/> where T equals <see cref="RecordType"/>.</value>
        public Type RecordWriterType
        {
            get { return _recordWriterType; }
        }

        /// <summary>
        /// Gets the type of the records written to the output.
        /// </summary>
        /// <value>
        /// A <see cref="Type"/> instance for the type of the records.
        /// </value>
        public Type RecordType
        {
            get { return _recordType; }
            set
            {
                if( _recordType != null )
                    throw new InvalidOperationException("Record type is already set.");
                _recordWriterType = _recordWriterType.MakeGenericType(value);
                _recordType = value;
            }
        }

        /// <summary>
        /// Gets or sets the block size in bytes for the output files.
        /// </summary>
        /// <value>The size of the block in bytes, or 0 to use the DFS default setting. The default value is 0.</value>
        public int BlockSize { get; set; }

        /// <summary>
        /// Gets or sets the replication factor for the output files.
        /// </summary>
        /// <value>The replication factor, or 0 to the use the DFS default setting. The default value is 0.</value>
        public int ReplicationFactor { get; set; }

        /// <summary>
        /// Gets or sets the record options for the output files.
        /// </summary>
        /// <value>A combination of values from the <see cref="RecordStreamOptions"/> enumeration. The default value is <see cref="RecordStreamOptions.None"/>.</value>
        public RecordStreamOptions RecordOptions { get; set; }
    }
}
