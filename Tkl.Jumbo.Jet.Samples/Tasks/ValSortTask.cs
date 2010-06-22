// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Samples.IO;

namespace Tkl.Jumbo.Jet.Samples.Tasks
{
    /// <summary>
    /// Task that validates the sort order in its input.
    /// </summary>
    public class ValSortTask : Configurable, IPullTask<GenSortRecord, ValSortRecord>
    {
        private Crc32 _crc = new Crc32();

        #region IPullTask<GenSortRecord,ValSortRecord> Members

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(RecordReader<GenSortRecord> input, RecordWriter<ValSortRecord> output)
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

            TaskDfsInput dfsInput = TaskContext.StageConfiguration.DfsInputs[TaskContext.TaskId.TaskNumber - 1];
            ValSortRecord result = new ValSortRecord()
            {
                InputId = dfsInput.Path + "_" + dfsInput.Block.ToString("00000"),
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
