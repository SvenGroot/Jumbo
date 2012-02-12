// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs.FileSystem;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Input
{
    /// <summary>
    /// Provides methods to create <see cref="FileStageInput{TRecordReader}"/> instances.
    /// </summary>
    public static class FileStageInput
    {
        /// <summary>
        /// Creates a <see cref="FileStageInput{TRecordReader}"/> for the specified record reader type.
        /// </summary>
        /// <param name="recordReaderType">Type of the record reader.</param>
        /// <param name="fileSystem">The file system containing the files.</param>
        /// <param name="fileOrDirectory">The input file or directory.</param>
        /// <param name="minSplitSize">The minimum split size.</param>
        /// <param name="maxSplitSize">The maximum split size.</param>
        /// <returns>The <see cref="FileStageInput{TRecordReader}"/></returns>
        public static IStageInput Create(Type recordReaderType, FileSystemClient fileSystem, JumboFileSystemEntry fileOrDirectory, int minSplitSize = 1, int maxSplitSize = Int32.MaxValue)
        {
            if( recordReaderType == null )
                throw new ArgumentNullException("recordReaderType");
            if( fileOrDirectory == null )
                throw new ArgumentNullException("fileOrDirectory");
            return (IStageInput)Activator.CreateInstance(typeof(FileStageInput<>).MakeGenericType(recordReaderType), fileSystem, fileOrDirectory, minSplitSize, maxSplitSize);
        }

        /// <summary>
        /// Creates a <see cref="FileStageInput{TRecordReader}"/> for the specified record reader type.
        /// </summary>
        /// <param name="recordReaderType">Type of the record reader.</param>
        /// <param name="fileSystem">The file system containing the files.</param>
        /// <param name="inputFiles">The input files.</param>
        /// <param name="minSplitSize">The minimum split size.</param>
        /// <param name="maxSplitSize">The maximum split size.</param>
        /// <returns>The <see cref="FileStageInput{TRecordReader}"/></returns>
        public static IStageInput Create(Type recordReaderType, FileSystemClient fileSystem, IEnumerable<JumboFile> inputFiles, int minSplitSize = 1, int maxSplitSize = Int32.MaxValue)
        {
            if( recordReaderType == null )
                throw new ArgumentNullException("recordReaderType");
            if( inputFiles == null )
                throw new ArgumentNullException("inputFiles");
            return (IStageInput)Activator.CreateInstance(typeof(FileStageInput<>).MakeGenericType(recordReaderType), fileSystem, inputFiles, minSplitSize, maxSplitSize);
        }
    }

    /// <summary>
    /// Provides a stage with input from a file system.
    /// </summary>
    /// <typeparam name="TRecordReader">The type of the record reader.</typeparam>
    public class FileStageInput<TRecordReader> : IStageInput
        where TRecordReader : IRecordReader
    {
        private readonly List<ITaskInput> _taskInputs;
        private const double _splitSlack = 1.1;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileStageInput&lt;TRecordReader&gt;"/> class.
        /// </summary>
        public FileStageInput()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileStageInput&lt;TRecordReader&gt;"/> class.
        /// </summary>
        /// <param name="fileSystem">The file system containing the files.</param>
        /// <param name="fileOrDirectory">The input file or directory.</param>
        /// <param name="minSplitSize">The minimum split size.</param>
        /// <param name="maxSplitSize">The maximum split size.</param>
        public FileStageInput(FileSystemClient fileSystem, JumboFileSystemEntry fileOrDirectory, int minSplitSize = 1, int maxSplitSize = Int32.MaxValue)
            : this(fileSystem, EnumerateFiles(fileOrDirectory), minSplitSize, maxSplitSize)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileStageInput&lt;TRecordReader&gt;"/> class.
        /// </summary>
        /// <param name="fileSystem">The file system containing the files.</param>
        /// <param name="inputFiles">The input files.</param>
        /// <param name="minSplitSize">The minimum split size.</param>
        /// <param name="maxSplitSize">The maximum split size.</param>
        public FileStageInput(FileSystemClient fileSystem, IEnumerable<JumboFile> inputFiles, int minSplitSize = 1, int maxSplitSize = Int32.MaxValue)
        {
            if( fileSystem == null )
                throw new ArgumentNullException("fileSystem");
            if( inputFiles == null )
                throw new ArgumentNullException("inputFiles");
            if( maxSplitSize <= 0 )
                throw new ArgumentOutOfRangeException("maxSplitSize");
            if( minSplitSize <= 0 )
                throw new ArgumentOutOfRangeException("minSplitSize");
            if( minSplitSize > maxSplitSize )
                throw new ArgumentException("Minimum split size must be less than or equal to maximum split size.");

            DfsClient dfsClient = fileSystem as DfsClient;
            List<FileTaskInput> taskInputs = new List<FileTaskInput>();
            foreach( JumboFile file in inputFiles )
            {
                if( file.Size > 0 ) // Don't create splits for zero-length files
                {
                    int splitSize = Math.Max(minSplitSize, (int)Math.Min(maxSplitSize, file.BlockSize));

                    long offset;
                    for( offset = 0; offset + (splitSize * _splitSlack) < file.Size; offset += splitSize )
                    {
                        taskInputs.Add(new FileTaskInput(file.FullPath, offset, splitSize, GetSplitLocations(dfsClient, file, offset)));
                    }

                    taskInputs.Add(new FileTaskInput(file.FullPath, offset, file.Size - offset, GetSplitLocations(dfsClient, file, offset)));
                }
            }

            if( taskInputs.Count == 0 )
                throw new ArgumentException("The specified input path contains no non-empty splits.", "inputFiles");
            // Sort by descending split size, so biggest splits are done first. Using OrderBy because that does a stable sort.
            _taskInputs = taskInputs.OrderByDescending(input => input.Size).Cast<ITaskInput>().ToList();
        }

        /// <summary>
        /// Gets the type of the records of this input.
        /// </summary>
        /// <value>
        /// The type of the records.
        /// </value>
        public Type RecordType
        {
            get { return typeof(TRecordReader).FindGenericBaseType(typeof(RecordReader<>), true).GetGenericArguments()[0]; } 
        }

        /// <summary>
        /// Gets the inputs for each task.
        /// </summary>
        /// <value>
        /// A list of task inputs, or <see langword="null"/> if the job is not being constructed. The returned collection may be read-only.
        /// </value>
        public IList<ITaskInput> TaskInputs
        {
            get { return _taskInputs == null ? null : _taskInputs.AsReadOnly(); }
        }

        /// <summary>
        /// Creates the record reader for the specified task.
        /// </summary>
        /// <param name="fileSystem">The file system.</param>
        /// <param name="jetConfiguration">The Jumbo Jet configuration. May be <see langword="null"/>.</param>
        /// <param name="context">The task context. May be <see langword="null"/>.</param>
        /// <param name="input">The task input.</param>
        /// <returns>
        /// The record reader.
        /// </returns>
        public IRecordReader CreateRecordReader(FileSystemClient fileSystem, JetConfiguration jetConfiguration, TaskContext context, ITaskInput input)
        {
            if( fileSystem == null )
                throw new ArgumentNullException("fileSystem");
            if( input == null )
                throw new ArgumentNullException("input");

            FileTaskInput fileInput = (FileTaskInput)input;
            return (IRecordReader)JetActivator.CreateInstance(typeof(TRecordReader), fileSystem.Configuration, jetConfiguration, context, fileSystem.OpenFile(fileInput.Path), fileInput.Offset, fileInput.Size, context == null ? false : context.AllowRecordReuse);
        }
        
        private static IEnumerable<string> GetSplitLocations(DfsClient dfsClient, JumboFile file, long offset)
        {
            if( dfsClient != null )
            {
                int blockIndex = (int)(offset / file.BlockSize);
                Guid blockId = file.Blocks[blockIndex];
                return dfsClient.NameServer.GetDataServersForBlock(blockId).Select(server => server.HostName);
            }

            return null;
        }

        private static IEnumerable<JumboFile> EnumerateFiles(JumboFileSystemEntry entry)
        {
            if( entry == null )
                throw new ArgumentNullException("entry");

            JumboDirectory directory = entry as JumboDirectory;
            if( directory != null )
            {
                return from child in directory.Children
                       let file = child as JumboFile
                       where file != null
                       select file;
            }
            else
            {
                return new[] { (JumboFile)entry };
            }
        }
    }
}
