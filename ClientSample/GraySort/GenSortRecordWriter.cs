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
        }

        protected override void WriteRecordInternal(GenSortRecord record)
        {
            _writer.Write(record.Key);
            _writer.Write(record.Value);
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                if( disposing )
                {
                    _writer.Dispose();
                }
            }
            finally
            {
                base.Dispose(disposing);
            }
        }
    }
}
