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
        private UInt128 _firstUnsorted = UInt128.Zero;
        private UInt128 _checksum = UInt128.Zero;
        private UInt128 _unsortedRecords = UInt128.Zero;
        private UInt128 _duplicates = UInt128.Zero;

        #region IPushTask<ValSortRecord,StringWritable> Members

        public void ProcessRecord(ValSortRecord record, RecordWriter<StringWritable> output)
        {
            if( _prev != null )
            {
                int diff = StringComparer.Ordinal.Compare(_prev.LastKey, record.FirstKey);
                if( diff == 0 )
                    ++_duplicates;
                else if( diff > 0 )
                    ++_unsortedRecords;
            }
            _unsortedRecords += record.UnsortedRecords;
            _checksum += record.Checksum;
            _duplicates += record.Duplicates;

            _prev = record;
        }

        public void Finish(RecordWriter<StringWritable> output)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
