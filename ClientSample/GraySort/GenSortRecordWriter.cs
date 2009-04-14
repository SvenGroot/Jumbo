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
        private readonly StreamWriter _writer;

        public GenSortRecordWriter(Stream stream)
            : base(stream)
        {
            _writer = new StreamWriter(stream, Encoding.ASCII);
            _writer.AutoFlush = true;
        }

        protected override void WriteRecordInternal(GenSortRecord record)
        {
            Debug.Assert(record.Key.Length == 10);
            Debug.Assert(record.Value.Length == 90);
            if( record.Key == " ]P/~W@Y#f" )
                Debugger.Break();
            _writer.Write(record.Key);
            _writer.Write(record.Value);
            Debug.Assert(_writer.BaseStream.Length % 100 == 0);
        }
    }
}
