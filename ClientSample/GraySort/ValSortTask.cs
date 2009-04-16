using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Dfs;

namespace ClientSample.GraySort
{
    public class ValSortTask : Configurable, IPullTask<GenSortRecord, ValSortRecord>
    {
        private Crc32 _crc = new Crc32();
        private byte[] _recordBuffer = new byte[100];

        #region IPullTask<GenSortRecord,ValSortRecord> Members

        public void Run(Tkl.Jumbo.IO.RecordReader<GenSortRecord> input, Tkl.Jumbo.IO.RecordWriter<ValSortRecord> output)
        {   
            long recordCrc;
            UInt128 checksum = UInt128.Zero;
            UInt128 duplicates = UInt128.Zero;
            UInt128 unsorted = UInt128.Zero;
            UInt128 count = UInt128.Zero;
            GenSortRecord first = null;
            GenSortRecord prev = null;
            UInt128? firstUnordered = null;
            foreach( GenSortRecord record in input.EnumerateRecords() )
            {
                recordCrc = CalculateCrc(record);
                checksum += new UInt128(0, (ulong)recordCrc);
                if( prev == null )
                {
                    first = record;
                }
                else
                {
                    int diff = prev.CompareTo(record);
                    if( diff == 0 )
                        ++duplicates;
                    else if( diff > 0 )
                    {
                        if( firstUnordered == null )
                            firstUnordered = count;
                        ++unsorted;
                    }
                }
                prev = record;
                ++count;
            }

            ValSortRecord result = new ValSortRecord()
            {
                TaskId = TaskConfiguration.TaskID,
                FirstKey = first.Key,
                LastKey = prev.Value,
                Records = count,
                UnsortedRecords = unsorted,
                FirstUnsorted = firstUnordered == null ? firstUnordered.Value : UInt128.Zero,
                Checksum = checksum,
                Duplicates = duplicates
            };
            output.WriteRecord(result);
        }

        #endregion

        private long CalculateCrc(GenSortRecord record)
        {
            _crc.Reset();
            Encoding.ASCII.GetBytes(record.Key, 0, 10, _recordBuffer, 0);
            Encoding.ASCII.GetBytes(record.Value, 0, 90, _recordBuffer, 10);
            _crc.Update(_recordBuffer);
            return _crc.Value;
        }
    }
}
