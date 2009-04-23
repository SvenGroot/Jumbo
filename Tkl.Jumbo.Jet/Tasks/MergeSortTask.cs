﻿using System;
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
        /// <summary>
        /// The name of the setting in <see cref="TaskConfiguration.TaskSettings"/> that specified the maximum number
        /// of files to merge in one pass.
        /// </summary>
        public const string MaxMergeInputsSetting = "MergeSortTask.MaxMergeTasks";
        /// <summary>
        /// The default maximum number of files to merge in one pass.
        /// </summary>
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
            int maxMergeInputs = TaskAttemptConfiguration.TaskConfiguration.GetTypedSetting(MaxMergeInputsSetting, 0);
            if( maxMergeInputs == 0 )
                maxMergeInputs = TaskAttemptConfiguration.JobConfiguration.GetTypedSetting(MaxMergeInputsSetting, DefaultMaxMergeInputs);

            if( maxMergeInputs <= 0 )
                throw new InvalidOperationException("maxMergeInputs must be larger than zero.");
            _log.InfoFormat("Merging {0} inputs with max {1} inputs per pass.", input.TotalInputCount, maxMergeInputs);

            int processed = 0;
            string previousMergePassOutputFile = null;
            while( processed < input.TotalInputCount )
            {
                input.WaitForInputs(processed + maxMergeInputs, Timeout.Infinite);
                int pass = 1;
                using( RecordReader<T> previousMergePassOutput = previousMergePassOutputFile == null ? null : new BinaryRecordReader<T>(previousMergePassOutputFile, TaskAttemptConfiguration.AllowRecordReuse, JetConfiguration.FileChannel.DeleteIntermediateFiles, JetConfiguration.FileChannel.MergeTaskReadBufferSize) )
                {
                    PriorityQueue<MergeInput> queue = new PriorityQueue<MergeInput>(EnumerateInputs(previousMergePassOutput, input, processed, maxMergeInputs), new MergeInputComparer());
                    processed += maxMergeInputs;
                    _log.InfoFormat("Merge pass {0}: merging {1} inputs.", pass, queue.Count);

                    RecordWriter<T> writer = null;
                    bool disposeWriter = false;
                    try
                    {
                        if( processed < input.TotalInputCount )
                        {
                            previousMergePassOutputFile = Path.Combine(TaskAttemptConfiguration.LocalJobDirectory, string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}_{1}_pass{2}.mergeoutput.tmp", TaskAttemptConfiguration.TaskConfiguration.TaskID, TaskAttemptConfiguration.Attempt, pass));
                            disposeWriter = true;
                            writer = new BinaryRecordWriter<T>(new FileStream(previousMergePassOutputFile, FileMode.CreateNew, FileAccess.Write, FileShare.None, JetConfiguration.FileChannel.MergeTaskReadBufferSize));
                            ++pass;
                        }
                        else
                        {
                            _log.Info("This is the final pass.");
                            writer = output;
                        }

                        while( queue.Count > 0 )
                        {
                            MergeInput front = queue.Peek();
                            writer.WriteRecord(front.Value);
                            T nextItem;
                            if( front.Reader.ReadRecord(out nextItem) )
                            {
                                front.Value = nextItem;
                                queue.AdjustFirstItem();
                            }
                            else
                                queue.Dequeue();
                        }
                    }
                    finally
                    {
                        if( disposeWriter && writer != null )
                        {
                            writer.Dispose();
                            writer = null;
                        }
                    }
                }
            }
        }

        #endregion

        private static IEnumerable<MergeInput> EnumerateInputs(RecordReader<T> previousMergePassOutput, MergeTaskInput<T> input, int start, int count)
        {
            if( previousMergePassOutput != null )
            {
                T item;
                if( previousMergePassOutput.ReadRecord(out item) )
                {
                    yield return new MergeInput() { Reader = previousMergePassOutput, Value = item };
                }
            }
            int end = Math.Min(start + count, input.Count);
            for( int x = start; x < end; ++x )
            {
                RecordReader<T> reader = input[x];
                T item;
                if( reader.ReadRecord(out item) )
                {
                    yield return new MergeInput() { Reader = reader, Value = item };
                }
            }
        }
    }
}
