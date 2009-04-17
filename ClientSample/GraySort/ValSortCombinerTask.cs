using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.IO;

namespace ClientSample.GraySort
{
    public class ValSortCombinerTask : IPushTask<ValSortRecord, StringWritable>
    {
        private ValSortRecord _prev;
        private UInt128 _checksum = UInt128.Zero;
        private UInt128 _unsortedRecords = UInt128.Zero;
        private UInt128 _duplicates = UInt128.Zero;
        private UInt128 _records = UInt128.Zero;
        private UInt128? _firstUnsorted;

        #region IPushTask<ValSortRecord,StringWritable> Members

        public void ProcessRecord(ValSortRecord record, RecordWriter<StringWritable> output)
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

        public void Finish(RecordWriter<StringWritable> output)
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
                output.WriteRecord("SUCCESS - All records are in order");
            }
            else
            {
                output.WriteRecord(string.Format("ERROR - there are {0} unordered records", _unsortedRecords));
            }
        }

        #endregion
    }
}
