using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Threading;
using System.IO;

namespace Tkl.Jumbo.Jet.Tasks
{
    /// <summary>
    /// Merges several sorted inputs into one sorted input.
    /// </summary>
    /// <typeparam name="T">The type of record.</typeparam>
    /// <remarks>
    /// <note>
    ///   The class that generates the input for this task (which can be either another task if a pipeline channel is used, or a <see cref="RecordReader{T}"/>)
    ///   may not reuse the <see cref="IWritable"/> instances for the records.
    /// </note>
    /// <para>
    ///   The inputs must individually be sorted, otherwise the result of this task is undefined.
    /// </para>
    /// </remarks>
    [AllowRecordReuse(PassThrough=true)]
    public class MergeSortTask<T> : Configurable, IMergeTask<T, T>
        where T : IWritable, new()
    {
        #region Nested types

        private class PreviousMergePassOutput
        {
            public string File { get; set; }
            public long UncompressedSize { get; set; }
        }

        #endregion

        /// <summary>
        /// The name of the setting in <see cref="StageConfiguration.StageSettings"/> that specified the maximum number
        /// of files to merge in one pass.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1000:DoNotDeclareStaticMembersOnGenericTypes")]
        public const string MaxMergeInputsSetting = "MergeSortTask.MaxMergeTasks";
        /// <summary>
        /// The default maximum number of files to merge in one pass.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1000:DoNotDeclareStaticMembersOnGenericTypes")]
        public const int DefaultMaxMergeInputs = 100;

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(MergeSortTask<T>));

        private class MergeInput
        {
            public T Value { get; set; }
            public RecordReader<T> Reader { get; set; }
        }

        private class MergeInputComparer : Comparer<MergeInput>
        {
            private readonly Comparer<T> _comparer = Comparer<T>.Default;

            public override int Compare(MergeInput x, MergeInput y)
            {
                return _comparer.Compare(x.Value, y.Value);
            }
        }

        #region IMergeTask<T,T> Members

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A list of <see cref="RecordReader{T}"/> instances from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(MergeTaskInput<T> input, RecordWriter<T> output)
        {
            int maxMergeInputs = TaskAttemptConfiguration.StageConfiguration.GetTypedSetting(MaxMergeInputsSetting, 0);
            if( maxMergeInputs == 0 )
                maxMergeInputs = TaskAttemptConfiguration.JobConfiguration.GetTypedSetting(MaxMergeInputsSetting, DefaultMaxMergeInputs);

            if( maxMergeInputs <= 0 )
                throw new InvalidOperationException("maxMergeInputs must be larger than zero.");
            _log.InfoFormat("Merging {0} inputs with max {1} inputs per pass.", input.TotalInputCount, maxMergeInputs);

            int processed = 0;
            List<PreviousMergePassOutput> previousMergePassOutputFiles = new List<PreviousMergePassOutput>();
            int pass = 1;
            int mergeOutputsProcessed = 0;
            while( processed < input.TotalInputCount || mergeOutputsProcessed < previousMergePassOutputFiles.Count )
            {
                List<RecordReader<T>> previousMergePassOutputs = null;
                RecordWriter<T> writer = null;
                bool disposeWriter = false;
                PreviousMergePassOutput currentOutput = null;
                try
                {
                    // Wait until we have enough inputs for another merge pass.
                    input.WaitForInputs(processed + maxMergeInputs, Timeout.Infinite);

                    // Create the merge queue from the new inputs.
                    PriorityQueue<MergeInput> queue = new PriorityQueue<MergeInput>(EnumerateInputs(input, processed, maxMergeInputs), new MergeInputComparer());
                    processed += queue.Count;
                    // If the queue size is smaller than the amount of inputs we can merge, and we have previous merge results, we'll add those.
                    if( queue.Count < maxMergeInputs && previousMergePassOutputFiles.Count > 0 )
                    {
                        previousMergePassOutputs = new List<RecordReader<T>>();
                        while( queue.Count < maxMergeInputs && mergeOutputsProcessed < previousMergePassOutputFiles.Count )
                        {
                            PreviousMergePassOutput previousOutput = previousMergePassOutputFiles[mergeOutputsProcessed];
                            RecordReader<T> reader = new BinaryRecordReader<T>(previousOutput.File, TaskAttemptConfiguration.AllowRecordReuse, JetConfiguration.FileChannel.DeleteIntermediateFiles, JetConfiguration.FileChannel.MergeTaskReadBufferSize, input.CompressionType, previousOutput.UncompressedSize);
                            previousMergePassOutputs.Add(reader);
                            if( reader.ReadRecord() )
                            {
                                queue.Enqueue(new MergeInput() { Value = reader.CurrentRecord, Reader = reader });
                            }
                            ++mergeOutputsProcessed;
                        }
                    }
                    _log.InfoFormat("Merge pass {0}: merging {1} inputs.", pass, queue.Count);

                    if( processed < input.TotalInputCount )
                    {
                        string outputFile = Path.Combine(TaskAttemptConfiguration.LocalJobDirectory, string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}_pass{1}.mergeoutput.tmp", TaskAttemptConfiguration.TaskAttemptId, pass));
                        _log.InfoFormat("Creating file {0} to hold output of pass {1}.", outputFile, pass);
                        disposeWriter = true;
                        writer = new BinaryRecordWriter<T>(new FileStream(outputFile, FileMode.CreateNew, FileAccess.Write, FileShare.None, JetConfiguration.FileChannel.MergeTaskReadBufferSize).CreateCompressor(input.CompressionType));
                        currentOutput = new PreviousMergePassOutput() { File = outputFile };
                        previousMergePassOutputFiles.Add(currentOutput);
                        ++pass;
                    }
                    else
                    {
                        _log.Info("This is the final pass.");
                        writer = output;
                    }

                    MergeQueue(queue, writer);

                    if( currentOutput != null )
                        currentOutput.UncompressedSize = writer.BytesWritten;
                }
                finally
                {
                    if( disposeWriter && writer != null )
                    {
                        writer.Dispose();
                        writer = null;
                    }
                    if( previousMergePassOutputs != null )
                    {
                        foreach( RecordReader<T> reader in previousMergePassOutputs )
                        {
                            reader.Dispose();
                        }
                        previousMergePassOutputs = null;
                    }
                }
            }
        }

        private static void MergeQueue(PriorityQueue<MergeInput> queue, RecordWriter<T> writer)
        {
            // merge the contents of the queue.
            while( queue.Count > 0 )
            {
                MergeInput front = queue.Peek();
                writer.WriteRecord(front.Value);
                if( front.Reader.ReadRecord() )
                {
                    front.Value = front.Reader.CurrentRecord;
                    queue.AdjustFirstItem();
                }
                else
                    queue.Dequeue();
            }
        }

        #endregion

        private static IEnumerable<MergeInput> EnumerateInputs(MergeTaskInput<T> input, int start, int count)
        {
            int end = Math.Min(start + count, input.Count);
            for( int x = start; x < end; ++x )
            {
                RecordReader<T> reader = input[x];
                if( reader.ReadRecord() )
                {
                    yield return new MergeInput() { Reader = reader, Value = reader.CurrentRecord };
                }
            }
        }
    }
}
