using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;
using System.Diagnostics;

namespace ClientSample.GraySort
{
    public class GenSortRecordWriter : StreamRecordWriter<GenSortRecord>
    {
        public GenSortRecordWriter(Stream stream)
            : base(stream)
        {
        }

        protected override void WriteRecordInternal(GenSortRecord record)
        {
            Stream.Write(record.RecordBuffer, 0, GenSortRecord.RecordSize);
        }
    }
}
