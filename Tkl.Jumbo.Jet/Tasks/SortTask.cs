// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Tasks
{
    /// <summary>
    /// Performs an in-memory sort of its input records. The sorting algorithm used is QuickSort.
    /// </summary>
    /// <typeparam name="T">The record type.</typeparam>
    /// <remarks>
    /// <note>
    ///   The class that generates the input for this task (which can be either another task if a pipeline channel is used, or a <see cref="RecordReader{T}"/>)
    ///   may not reuse the <see cref="IWritable"/> instances for the records.
    /// </note>
    /// </remarks>
    public class SortTask<T> : Configurable, IPrepartitionedPushTask<T, T>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(SortTask<T>));
        private List<T>[] _partitions;
        private IComparer<T> _comparer;

        /// <summary>
        /// Indicates the configuration has been changed. <see cref="JetActivator.ApplyConfiguration"/> calls this method
        /// after setting the configuration.
        /// </summary>
        public override void NotifyConfigurationChanged()
        {
            _comparer = null;
            if( TaskAttemptConfiguration != null )
            {
                string comparerTypeName = TaskAttemptConfiguration.StageConfiguration.GetSetting(SortTaskConstants.ComparerSetting, null);
                if( !string.IsNullOrEmpty(comparerTypeName) )
                    _comparer = (IComparer<T>)JetActivator.CreateInstance(Type.GetType(comparerTypeName, true), DfsConfiguration, JetConfiguration, TaskAttemptConfiguration);
                _partitions = new List<T>[TaskAttemptConfiguration.StageConfiguration.InternalPartitionCount];
            }
            else
                _partitions = new List<T>[1];

            for( int x = 0; x < _partitions.Length; ++x )
                _partitions[x] = new List<T>();

            if( _comparer == null )
                _comparer = Comparer<T>.Default;
        }

        #region IPushTask<TInput,TOutput> Members

        /// <summary>
        /// Method called for each record in the task's input.
        /// </summary>
        /// <param name="record">The record to process.</param>
        /// <param name="partition">The partition of the record</param>
        /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void ProcessRecord(T record, int partition, PrepartitionedRecordWriter<T> output)
        {
            _partitions[partition].Add(record);
        }

        /// <summary>
        /// Method called after the last record was processed.
        /// </summary>
        /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Finish(PrepartitionedRecordWriter<T> output)
        {
            if( output == null )
                throw new ArgumentNullException("output");
            for( int partition = 0; partition < _partitions.Length; ++partition )
            {
                List<T> records = _partitions[partition];
                _log.InfoFormat("Sorting {0} records for partition {1}.", records.Count, partition);
                records.Sort(_comparer);
                _log.Info("Sort complete.");
                foreach( T record in records )
                {
                    output.WriteRecord(record, partition);
                }
            }
        }

        #endregion
    }
}
