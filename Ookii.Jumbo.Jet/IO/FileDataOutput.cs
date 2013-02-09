// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Jobs;
using System.Globalization;
using System.IO;

namespace Ookii.Jumbo.Jet.IO
{
    /// <summary>
    /// Provides methods for creating instances of the <see cref="FileDataOutput{TRecordWriter}"/> class.
    /// </summary>
    public static class FileDataOutput
    {
        /// <summary>
        /// The key of the setting in the stage settings that stores the output path format for the <see cref="FileDataOutput{TRecordWriter}"/> class. You should not normally change this setting.
        /// </summary>
        public const string OutputPathFormatSettingKey = "FileDataOutput.OutputPathFormat";
        /// <summary>
        /// The key of the setting in the stage settings that stores the block size for the <see cref="FileDataOutput{TRecordWriter}"/> class. You should not normally change this setting.
        /// </summary>
        public const string BlockSizeSettingKey = "FileDataOutput.BlockSizeSettingKey";
        /// <summary>
        /// The key of the setting in the stage settings that stores the replication factor  for the <see cref="FileDataOutput{TRecordWriter}"/> class. You should not normally change this setting.
        /// </summary>
        public const string ReplicationFactorSettingKey = "FileDataOutput.ReplicationFactor";
        /// <summary>
        /// The key of the setting in the stage settings that stores the record options for the <see cref="FileDataOutput{TRecordWriter}"/> class. You should not normally change this setting.
        /// </summary>
        public const string RecordOptionsSettingKey = "FileDataOutput.RecordOptions";

        /// <summary>
        /// Creates an instance of the <see cref="FileDataOutput{TRecordWriter}"/> class for the specified record writer type.
        /// </summary>
        /// <param name="recordWriterType">Type of the record writer.</param>
        /// <param name="fileSystem">The file system that the output will be written to..</param>
        /// <param name="outputPath">The path of the directory to write the output to.</param>
        /// <param name="blockSize">The size of the output files' blocks, or 0 to use the default block size.</param>
        /// <param name="replicationFactor">The output files' replication factor, or 0 to use the default replication factor.</param>
        /// <param name="recordOptions">The <see cref="RecordStreamOptions"/> for the output.</param>
        /// <returns>An instance of the <see cref="FileDataOutput{TRecordWriter}"/> class.</returns>
        public static IDataOutput Create(Type recordWriterType, FileSystemClient fileSystem, string outputPath, int blockSize = 0, int replicationFactor = 0, RecordStreamOptions recordOptions = RecordStreamOptions.None)
        {
            if( recordWriterType == null )
                throw new ArgumentNullException("recordWriterType");

            return (IDataOutput)Activator.CreateInstance(typeof(FileDataOutput<>).MakeGenericType(recordWriterType), fileSystem, outputPath, blockSize, replicationFactor, recordOptions);
        }
    }

    /// <summary>
    /// Writes stage output to a file system.
    /// </summary>
    /// <typeparam name="TRecordWriter">The type of the record writer.</typeparam>
    public class FileDataOutput<TRecordWriter> : IDataOutput
        where TRecordWriter : IRecordWriter
    {
        private readonly FileSystemClient _fileSystem;
        private readonly string _outputPath;
        private readonly int _blockSize;
        private readonly int _replicationFactor;
        private readonly RecordStreamOptions _recordOptions;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileDataOutput&lt;TRecordWriter&gt;"/> class.
        /// </summary>
        public FileDataOutput()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileDataOutput&lt;TRecordWriter&gt;"/> class.
        /// </summary>
        /// <param name="fileSystem">The file system that the output will be written to..</param>
        /// <param name="outputPath">The path of the directory to write the output to.</param>
        /// <param name="blockSize">The size of the output files' blocks, or 0 to use the default block size.</param>
        /// <param name="replicationFactor">The output files' replication factor, or 0 to use the default replication factor.</param>
        /// <param name="recordOptions">The <see cref="RecordStreamOptions"/> for the output.</param>
        public FileDataOutput(FileSystemClient fileSystem, string outputPath, int blockSize = 0, int replicationFactor = 0, RecordStreamOptions recordOptions = RecordStreamOptions.None)
        {
            if( fileSystem == null )
                throw new ArgumentNullException("fileSystem");
            if( outputPath == null )
                throw new ArgumentNullException("outputPath");
            if( blockSize < 0 )
                throw new ArgumentOutOfRangeException("blockSize");
            if( replicationFactor < 0 )
                throw new ArgumentOutOfRangeException("replicationFactor");
            if( fileSystem.GetDirectoryInfo(outputPath) == null )
                throw new DirectoryNotFoundException(string.Format(CultureInfo.CurrentCulture, "The directory '{0}' does not exist.", outputPath));

            _fileSystem = fileSystem;
            _outputPath = outputPath;
            _blockSize = blockSize;
            _replicationFactor = replicationFactor;
            _recordOptions = recordOptions;
        }

        /// <summary>
        /// Gets the type of the records used for this output.
        /// </summary>
        /// <value>
        /// The type of the records.
        /// </value>
        public Type RecordType
        {
            get { return typeof(TRecordWriter).FindGenericBaseType(typeof(RecordWriter<>), true).GetGenericArguments()[0]; }
        }

        /// <summary>
        /// Creates the output for the specified partition.
        /// </summary>
        /// <param name="fileSystem">The file system.</param>
        /// <param name="jetConfiguration">The Jumbo Jet configuration. May be <see langword="null"/>.</param>
        /// <param name="context">The task context.</param>
        /// <param name="partitionNumber">The partition number for this output.</param>
        /// <returns>
        /// The record writer.
        /// </returns>
        public IOutputCommitter CreateOutput(FileSystemClient fileSystem, JetConfiguration jetConfiguration, TaskContext context, int partitionNumber)
        {
            if( fileSystem == null )
                throw new ArgumentNullException("fileSystem");
            if( context == null )
                throw new ArgumentNullException("context");


            int blockSize = context.StageConfiguration.GetTypedSetting(FileDataOutput.BlockSizeSettingKey, 0);
            int replicationFactor = context.StageConfiguration.GetTypedSetting(FileDataOutput.ReplicationFactorSettingKey, 0);
            RecordStreamOptions recordOptions = context.StageConfiguration.GetTypedSetting(FileDataOutput.RecordOptionsSettingKey, RecordStreamOptions.None);

            // Must use TaskAttemptId for temp file name, there could be other attempts of this task writing the same data (only one will commit, of course).
            string tempFileName = fileSystem.Path.Combine(fileSystem.Path.Combine(context.DfsJobDirectory, "temp"), string.Format(CultureInfo.InvariantCulture, "{0}_partition{1}", context.TaskAttemptId, partitionNumber));
            string outputFileName = GetOutputPath(context.StageConfiguration, partitionNumber);
            Stream outputStream = fileSystem.CreateFile(tempFileName, blockSize, replicationFactor, recordOptions);
            IRecordWriter writer = (IRecordWriter)JetActivator.CreateInstance(typeof(TRecordWriter), fileSystem.Configuration, jetConfiguration, context, outputStream);
            return new FileOutputCommitter(writer, tempFileName, outputFileName);
        }

        /// <summary>
        /// Notifies the data input that it has been added to a stage.
        /// </summary>
        /// <param name="stage">The stage configuration of the stage.</param>
        public void NotifyAddedToStage(StageConfiguration stage)
        {
            if( stage == null )
                throw new ArgumentNullException("stage");
            if( _outputPath == null )
                throw new InvalidOperationException("No configuration is stored in this instance.");

            string outputPathFormat = _fileSystem.Path.Combine(_outputPath, stage.StageId + "-{0:00000}");
            stage.AddSetting(FileDataOutput.OutputPathFormatSettingKey, outputPathFormat);
            if( _blockSize != 0 )
                stage.AddTypedSetting(FileDataOutput.BlockSizeSettingKey, _blockSize);
            if( _replicationFactor != 0 )
                stage.AddTypedSetting(FileDataOutput.ReplicationFactorSettingKey, _replicationFactor);
            if( _recordOptions != RecordStreamOptions.None )
                stage.AddTypedSetting(FileDataOutput.RecordOptionsSettingKey, _recordOptions);
        }

        /// <summary>
        /// Gets the output path for the specified partition.
        /// </summary>
        /// <param name="stage">The stage configuration for the stage.</param>
        /// <param name="partitionNumber">The partition number.</param>
        /// <returns>The path of the output file for this partition.</returns>
        public string GetOutputPath(StageConfiguration stage, int partitionNumber)
        {
            if( stage == null )
                throw new ArgumentNullException("stage");
            string outputPathFormat = stage.GetSetting(FileDataOutput.OutputPathFormatSettingKey, null);
            if( outputPathFormat == null )
                throw new InvalidOperationException("The stage settings do not contain an output path format.");
            return string.Format(CultureInfo.InvariantCulture, outputPathFormat, partitionNumber);
        }
    }
}
