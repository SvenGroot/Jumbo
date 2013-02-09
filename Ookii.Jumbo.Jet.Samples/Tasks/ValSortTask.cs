// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Jet.Samples.IO;
using Ookii.Jumbo.Jet.IO;

namespace Ookii.Jumbo.Jet.Samples.Tasks
{
    /// <summary>
    /// Task that validates the sort order in its input.
    /// </summary>
    public class ValSortTask : Configurable, ITask<GenSortRecord, ValSortRecord>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(ValSortTask));

        private Crc32 _crc = new Crc32();

        #region ITask<GenSortRecord,ValSortRecord> Members

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

            FileTaskInput taskInput = (FileTaskInput)TaskContext.TaskInput;
            _log.InfoFormat("Input file {0} split offset {1} size {2} contains {3} unordered records.", taskInput.Path, taskInput.Offset, taskInput.Size, unsorted);

            ValSortRecord result = new ValSortRecord()
            {
                InputId = taskInput.Path,
                InputOffset = taskInput.Offset,
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
