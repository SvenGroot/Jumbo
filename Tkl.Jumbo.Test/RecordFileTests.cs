// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.IO;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Test
{
    [TestFixture]
    class RecordFileTests
    {
        [Test]
        public void TestRecordFileReaderWriter()
        {
            const int recordCount = 1000;
            List<string> records = Utilities.GenerateTextData(100, recordCount);

            byte[] data;
            long headerSize;
            using( MemoryStream stream = new MemoryStream() )
            {
                using( RecordFileWriter<StringWritable> writer = new RecordFileWriter<StringWritable>(stream) )
                {
                    Assert.AreEqual(typeof(StringWritable), writer.Header.RecordType);
                    Assert.AreEqual(typeof(StringWritable).FullName + ", " + typeof(StringWritable).Assembly.GetName().Name, writer.Header.RecordTypeName);
                    Assert.AreEqual(1, writer.Header.Version);
                    Assert.AreEqual(0, writer.RecordsWritten);
                    Assert.AreNotEqual(0, writer.BytesWritten); // Because it must've written the header this isn't 0.
                    headerSize = writer.BytesWritten;
                    StringWritable record = new StringWritable();
                    foreach( string item in records )
                    {
                        record.Value = item;
                        writer.WriteRecord(record);
                    }

                    Assert.AreEqual(recordCount, writer.RecordsWritten);
                }
                data = stream.ToArray();
            }

            const int recordSize = 105; // 100 ASCII characters + 4 byte prefix + 1 byte string length.
            const int totalRecordSize = recordSize * recordCount;
            // Hard-coded version 1 record marker distance and size + prefix size.
            long expectedSize = totalRecordSize + (totalRecordSize / 2000 * 20) + headerSize;
            Assert.AreEqual(expectedSize, data.Length);

            List<string> result = new List<string>(recordCount);
            const int stepSize = 10000;
            int totalRecordsRead = 0;
            for( int offset = 0; offset < data.Length; offset += stepSize )
            {
                using( MemoryStream stream = new MemoryStream(data) )
                using( RecordFileReader<StringWritable> reader = new RecordFileReader<StringWritable>(stream, offset, Math.Min(stepSize, stream.Length - offset), true) )
                {
                    Assert.AreEqual(typeof(StringWritable), reader.Header.RecordType);
                    Assert.AreEqual(typeof(StringWritable).FullName + ", " + typeof(StringWritable).Assembly.GetName().Name, reader.Header.RecordTypeName);
                    Assert.AreEqual(1, reader.Header.Version);
                    foreach( StringWritable record in reader.EnumerateRecords() )
                    {
                        result.Add(record.Value);
                    }
                    totalRecordsRead += reader.RecordsRead;
                }
            }

            Assert.AreEqual(recordCount, totalRecordsRead);
            Assert.IsTrue(Utilities.CompareList(records, result));
        }
    }
}
