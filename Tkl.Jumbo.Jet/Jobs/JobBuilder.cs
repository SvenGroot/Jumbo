using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Reflection;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Channels;
using System.IO;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Provides easy construction of Jumbo Jet jobs.
    /// </summary>
    public sealed class JobBuilder
    {
        #region Nested types

        private sealed class RecordReaderReference<T> : RecordReader<T>
            where T : IWritable, new()
        {
            public RecordReaderReference(JobBuilder jobBuilder, string input, Type recordReaderType)
            {
                JobBuilder = jobBuilder;
                Input = input;
                RecordReaderType = recordReaderType;
            }

            public Type RecordReaderType { get; private set; }

            public string Input { get; private set; }

            public JobBuilder JobBuilder { get; private set; }

            public override float Progress
            {
                get { throw new NotSupportedException(); }
            }

            protected override bool ReadRecordInternal()
            {
                throw new NotSupportedException();
            }
        }

        private sealed class RecordWriterReference<T> : RecordWriter<T>
            where T : IWritable, new()
        {
            public RecordWriterReference(JobBuilder jobBuilder, string output, Type recordWriterType)
            {
                JobBuilder = jobBuilder;
                Output = output;
                RecordWriterType = recordWriterType;
            }

            public string Output { get; private set; }

            public Type RecordWriterType { get; private set; }

            public JobBuilder JobBuilder { get; private set; }

            protected override void WriteRecordInternal(T record)
            {
                throw new NotSupportedException();
            }
        }

        #endregion

        private readonly JobConfiguration _job = new JobConfiguration();
        private readonly HashSet<Assembly> _assemblies = new HashSet<Assembly>();
        private readonly DfsClient _dfsClient = new DfsClient();
        private readonly JetClient _jetClient = new JetClient();

        /// <summary>
        /// Gets the job configuration.
        /// </summary>
        public JobConfiguration JobConfiguration
        {
            get
            {
                _assemblies.Remove(typeof(BasicJob).Assembly); // Don't include Tkl.Jumbo.Jet assembly
                _assemblies.Remove(typeof(RecordReader<>).Assembly); // Don't include Tkl.Jumbo assembly
                _job.AssemblyFileNames.Clear();
                _job.AssemblyFileNames.AddRange(from a in _assemblies select Path.GetFileName(a.Location));

                return _job;
            }
        }

        /// <summary>
        /// Gets the list of custom assemblies containing the task and record reader and writer types.
        /// </summary>
        public IEnumerable<Assembly> Assemblies
        {
            get 
            {
                _assemblies.Remove(typeof(BasicJob).Assembly); // Don't include Tkl.Jumbo.Jet assembly
                _assemblies.Remove(typeof(RecordReader<>).Assembly); // Don't include Tkl.Jumbo assembly
                return _assemblies.ToArray(); 
            }
        }

        /// <summary>
        /// Creates a record reader that reads data from the specified input.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <param name="input">The input file or directory on the DFS to read from.</param>
        /// <param name="recordReaderType">The type of the record reader to use.</param>
        /// <returns>An instance of a record reader. Note the return value is not necessarily of the type specified in <paramref name="recordReaderType"/>,
        /// so do not try to cast it.</returns>
        public RecordReader<T> CreateRecordReader<T>(string input, Type recordReaderType)
            where T : IWritable, new()
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( recordReaderType == null )
                throw new ArgumentNullException("recordReaderType");
            if( !recordReaderType.IsSubclassOf(typeof(RecordReader<T>)) )
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "recordReaderType does not specify a type that inherits from {0}", typeof(RecordReader<T>).FullName), "recordReaderType");

            return new RecordReaderReference<T>(this, input, recordReaderType);
        }

        /// <summary>
        /// Creates a record reader that writes data to the specified input.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <param name="output">The directory on the DFS to write to.</param>
        /// <param name="recordWriterType">The type of the record writer to use.</param>
        /// <returns>An instance of a record writer. Note the return value is not necessarily of the type specified in <paramref name="recordWriterType"/>,
        /// so do not try to cast it.</returns>
        public RecordWriter<T> CreateRecordWriter<T>(string output, Type recordWriterType)
            where T : IWritable, new()
        {
            if( output == null )
                throw new ArgumentNullException("output");
            if( recordWriterType == null )
                throw new ArgumentNullException("recordWriterType");
            if( !recordWriterType.IsSubclassOf(typeof(RecordWriter<T>)) )
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "recordWriterType does not specify a type that inherits from {0}", typeof(RecordWriter<T>).FullName), "recordReaderType");

            return new RecordWriterReference<T>(this, output, recordWriterType);
        }

        /// <summary>
        /// Processes records using the specified task type.
        /// </summary>
        /// <typeparam name="TInput">The input record type.</typeparam>
        /// <typeparam name="TOutput">The output record type.</typeparam>
        /// <param name="input">The record reader to read records to process from.</param>
        /// <param name="output">The record writer to write the result to.</param>
        /// <param name="taskType">The type of the task.</param>
        public void ProcessRecords<TInput, TOutput>(RecordReader<TInput> input, RecordWriter<TOutput> output, Type taskType)
            where TInput : IWritable, new()
            where TOutput : IWritable, new()
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( output == null )
                throw new ArgumentNullException("output");
            if( taskType == null )
                throw new ArgumentNullException("taskType");

            string outputPath = null;
            Type outputWriterType = null;
            RecordCollector<TOutput> collector = null;
            RecordWriterReference<TOutput> outputRef = output as RecordWriterReference<TOutput>;
            if( outputRef != null )
            {
                outputPath = outputRef.Output;
                outputWriterType = outputRef.RecordWriterType;
                _assemblies.Add(outputWriterType.Assembly);
            }
            else
            {
                collector = RecordCollector<TOutput>.GetCollector(output);
                if( collector != null )
                {
                    if( collector.InputStage != null )
                        throw new ArgumentException("Cannot write to the specified record writer, because that writer is already used by another stage.", "output");
                    _assemblies.Add(collector.PartitionerType.Assembly);
                }
                else
                    throw new ArgumentException("Unsupported output record writer.", "output");
            }

            RecordReaderReference<TInput> inputRef = input as RecordReaderReference<TInput>;
            StageConfiguration stage;
            if( inputRef != null )
            {
                // We're adding an input stage.
                stage = _job.AddInputStage(taskType.Name, _dfsClient.NameServer.GetFileSystemEntryInfo(inputRef.Input), taskType, inputRef.RecordReaderType, outputPath, outputWriterType);
                _assemblies.Add(inputRef.RecordReaderType.Assembly);
            }
            else
            {
                RecordCollector<TInput> inputCollector = RecordCollector<TInput>.GetCollector(input);
                if( inputCollector == null )
                    throw new ArgumentException("The specified record reader was not created by a JobBuilder or RecordCollector.", "input");
                if( inputCollector.InputStage == null )
                    throw new ArgumentException("Cannot read from the specified record reader because the associated RecordCollector isn't being written to.");
                // If the number of partitions is not specified, we will use the number of task servers in the Jet cluster as the task count.
                int taskCount = inputCollector.Partitions == null ?
                                (inputCollector.ChannelType == ChannelType.Pipeline ? 1 : _jetClient.JobServer.GetMetrics().TaskServers.Count) : 
                                inputCollector.Partitions.Value;
                // We default to the File channel if not specified.
                ChannelType channelType = inputCollector.ChannelType == null ? ChannelType.File : inputCollector.ChannelType.Value;

                stage = _job.AddStage(taskType.Name, new[] { inputCollector.InputStage }, taskType, taskCount, channelType, ChannelConnectivity.Full, null, inputCollector.PartitionerType, outputPath, outputWriterType);
            }

            if( collector != null )
                collector.InputStage = stage;

            _assemblies.Add(taskType.Assembly);
        }
    }
}
