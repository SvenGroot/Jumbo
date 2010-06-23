// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Represents input read from the DFS for a stage in a job being built by the <see cref="OldJobBuilder"/> class.
    /// </summary>
    public sealed class DfsInput : IStageInput
    {
        private readonly string _path;
        private Type _recordReaderType;
        private Type _recordType;

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsInput"/> class.
        /// </summary>
        /// <param name="path">The path of a directory or file on the DFS to read from.</param>
        /// <param name="recordReaderType">The type of the record reader to use. This must be a class inheriting from <see cref="RecordReader{T}"/>.</param>
        public DfsInput(string path, Type recordReaderType)
        {
            if( path == null )
                throw new ArgumentNullException("path");
            if( recordReaderType == null )
                throw new ArgumentNullException("recordReaderType");

            Type recordReaderBaseType = recordReaderType.FindGenericBaseType(typeof(RecordReader<>), true);

            _path = path;
            _recordReaderType = recordReaderType;
            if( !_recordReaderType.IsGenericTypeDefinition )
                _recordType = recordReaderBaseType.GetGenericArguments()[0];
        }

        /// <summary>
        /// Gets the path of a directory or file on the DFS that the input will be read from.
        /// </summary>
        /// <value>The path of a directory or file on the DFS.</value>
        public string Path
        {
            get { return _path; }
        }

        /// <summary>
        /// Gets the type of the record reader.
        /// </summary>
        /// <value>The <see cref="Type"/> instance for the record reader. This is a class inheriting from <see cref="RecordReader{T}"/> where T is <see cref="RecordType"/>.</value>
        public Type RecordReaderType
        {
            get { return _recordReaderType; }
        }

        /// <summary>
        /// Gets the type of the records read from the input.
        /// </summary>
        /// <value>
        /// A <see cref="Type"/> instance for the type of the records.
        /// </value>
        public Type RecordType
        {
            get { return _recordType; }
            internal set
            {
                if( _recordType != null )
                    throw new InvalidOperationException("Record type already set.");
                _recordReaderType = _recordReaderType.MakeGenericType(value);
                _recordType = value;
            }
        }
    }
}
