// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Samples.IO;

namespace Tkl.Jumbo.Jet.Samples.Tasks
{
    /// <summary>
    /// Task that checks whether the results of the <see cref="ValSortTask"/> are correctly
    /// sorted within themselves.
    /// </summary>
    /// <remarks>
    /// The input <see cref="ValSortRecord"/> records need to be sorted.
    /// </remarks>
    public class ValSortCombinerTask : IPushTask<ValSortRecord, string>
    {
        private ValSortRecord _prev;
        private UInt128 _checksum = UInt128.Zero;
        private UInt128 _unsortedRecords = UInt128.Zero;
        private UInt128 _duplicates = UInt128.Zero;
        private UInt128 _records = UInt128.Zero;
        private UInt128? _firstUnsorted;

        #region IPushTask<ValSortRecord,string> Members

        /// <summary>
        /// Method called for each record in the task's input.
        /// </summary>
        /// <param name="record">The record to process.</param>
        /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void ProcessRecord(ValSortRecord record, RecordWriter<string> output)
        {
            if( _prev != null )
            {
                int diff = GenSortRecord.CompareKeys(_prev.LastKey, record.FirstKey);
                if( diff == 0 )
                    ++_duplicates;
                else if( diff > 0 )
                {
                    if( _firstUnsorted == null )
                        _firstUnsorted = _records;
                    ++_unsortedRecords;
                }
            }
            _unsortedRecords += record.UnsortedRecords;
            _checksum += record.Checksum;
            _duplicates += record.Duplicates;
            if( _firstUnsorted == null && record.UnsortedRecords != UInt128.Zero )
            {
                _firstUnsorted = _records + record.FirstUnsorted;
            }
            _records += record.Records;

            _prev = record;
        }

        /// <summary>
        /// Method called after the last record was processed.
        /// </summary>
        /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        /// <remarks>
        /// This enables the task to finish up its processing and write any further records it may have collected during processing.
        /// </remarks>
        public void Finish(RecordWriter<string> output)
        {
            if( _unsortedRecords != UInt128.Zero )
            {
                output.WriteRecord(string.Format("First unordered record is record {0}", _firstUnsorted.Value));
            }
            output.WriteRecord(string.Format("Records: {0}", _records));
            output.WriteRecord(string.Format("Checksum: {0}", _checksum.ToHexString()));
            if( _unsortedRecords == UInt128.Zero )
            {
                output.WriteRecord(string.Format("Duplicate keys: {0}", _duplicates));
                output.WriteRecord("SUCCESS - all records are in order");
            }
            else
            {
                output.WriteRecord(string.Format("ERROR - there are {0} unordered records", _unsortedRecords));
            }
        }

        #endregion
    }
}
