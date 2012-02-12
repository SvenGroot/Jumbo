// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Dfs.FileSystem;
using Tkl.Jumbo.Jet.Input;

namespace Tkl.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// Represents input read from the DFS for a job being constructed by the <see cref="JobBuilder"/> class.
    /// </summary>
    public sealed class DfsInput : IOperationInput
    {
        private readonly string _path;
        private readonly Type _recordReaderType;
        private readonly Type _recordType;

        internal DfsInput(string path, Type recordReaderType)
        {
            if( path == null )
                throw new ArgumentNullException("path");
            if( recordReaderType == null )
                throw new ArgumentNullException("recordReaderType");
            if( recordReaderType.ContainsGenericParameters )
                throw new ArgumentException("The record reader type must be a closed constructed generic type.", "recordReaderType");

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
        }

        /// <summary>
        /// Creates an <see cref="IDataInput"/> for this input.
        /// </summary>
        /// <param name="fileSystem">The file system.</param>
        /// <returns></returns>
        public Input.IDataInput CreateStageInput(FileSystemClient fileSystem)
        {
            return FileDataInput.Create(RecordReaderType, fileSystem, fileSystem.GetFileSystemEntryInfo(Path));
        }
    }
}
