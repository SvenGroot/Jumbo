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
                TaskId = TaskConfiguration.DfsInput.Path + "_" + TaskConfiguration.DfsInput.Block.ToString("00000"),
                FirstKey = first.ExtractKeyBytes(),
                LastKey = prev.ExtractKeyBytes(),
                Records = count,
                UnsortedRecords = unsorted,
                FirstUnsorted = firstUnordered != null ? firstUnordered.Value : UInt128.Zero,
                Checksum = checksum,
                Duplicates = duplicates
            };
            output.WriteRecord(result);
        }

        #endregion

        private long CalculateCrc(GenSortRecord record)
        {
            _crc.Reset();
            _crc.Update(record.RecordBuffer);
            return _crc.Value;
        }
    }
}
