﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Dfs;
using System.Diagnostics;
using System.Threading;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Test.Dfs
{
    [TestFixture(Description="Tests reading and writing DFS data with record readers using various record stream options.")]
    [Category("ClusterTest")]
    public class RecordReaderWriterTests
    {
        private TestDfsCluster _cluster;
        private DfsClient _dfsClient;
        private List<Utf8String> _records;
        private const int _blockSize = 16 * (int)BinaryValue.Megabyte;

        [TestFixtureSetUp]
        public void Setup()
        {
            Trace.AutoFlush = true;
            _cluster = new TestDfsCluster(1, 1, _blockSize);
            Trace.WriteLine("Starting nameserver.");
            _dfsClient = TestDfsCluster.CreateClient();
            _dfsClient.NameServer.WaitForSafeModeOff(Timeout.Infinite);
            Trace.WriteLine("Name server running.");
            _records = Utilities.GenerateUtf8TextData(100000, 1000).ToList();
        }

        [TestFixtureTearDown]
        public void Teardown()
        {
            Trace.WriteLine("Shutting down cluster.");
            Trace.Flush();
            _cluster.Shutdown();
            Trace.WriteLine("Cluster shut down.");
            Trace.Flush();
        }

        [Test]
        public void TestLineRecordReader()
        {
            const string fileName = "/lines";
            int recordSize = _records[0].ByteLength + Environment.NewLine.Length;
            using( DfsOutputStream stream = _dfsClient.CreateFile(fileName, 0, 0) )
            using( TextRecordWriter<Utf8String> writer = new TextRecordWriter<Utf8String>(stream) )
            {
                foreach( Utf8String record in _records )
                    writer.WriteRecord(record);

                Assert.AreEqual(_records.Count, writer.RecordsWritten);
                Assert.AreEqual(_records.Count * recordSize, writer.OutputBytes);
                Assert.AreEqual(writer.OutputBytes, writer.BytesWritten);
                Assert.AreEqual(writer.OutputBytes, stream.Length);
            }

            int recordIndex = 0;
            DfsFile file = _dfsClient.NameServer.GetFileInfo(fileName);
            int blocks = file.Blocks.Count;
            int totalRecordsRead = 0;
            for( int block = 0; block < blocks; ++block )
            {
                int offset = block * _blockSize;
                int size = Math.Min((int)(file.Size - offset), _blockSize);
                using( DfsInputStream stream = _dfsClient.OpenFile(fileName) )
                using( LineRecordReader reader = new LineRecordReader(stream, block * _blockSize, size, true) )
                {
                    foreach( Utf8String record in reader.EnumerateRecords() )
                    {
                        Assert.AreEqual(_records[recordIndex], record);
                        ++recordIndex;
                    }

                    totalRecordsRead += reader.RecordsRead;
                    int firstRecord = offset == 0 ? 0 : (offset / recordSize) + 1;
                    int lastRecord = ((offset + size) / recordSize);
                    if( offset + size < file.Size )
                        ++lastRecord;
                    int recordCount = lastRecord - firstRecord;
                    Assert.AreEqual(recordCount, reader.RecordsRead);
                    Assert.AreEqual(recordCount * recordSize, reader.InputBytes);
                    Assert.GreaterOrEqual(reader.BytesRead, recordCount * recordSize + (recordSize - offset % recordSize));
                    Assert.AreEqual(stream.Position - offset, reader.BytesRead);
                    Assert.AreEqual(block == blocks - 1 ? 1 : 2, stream.BlocksRead);
                }
            }

            Assert.AreEqual(_records.Count, totalRecordsRead);
        }

        [Test]
        public void TestLineRecordReaderRecordsDoNotCrossBoundary()
        {
            const string fileName = "/linesboundary";
            int recordSize = _records[0].ByteLength + Environment.NewLine.Length;
            using( DfsOutputStream stream = _dfsClient.CreateFile(fileName, 0, 0, RecordStreamOptions.DoNotCrossBoundary) )
            using( TextRecordWriter<Utf8String> writer = new TextRecordWriter<Utf8String>(stream) )
            {
                foreach( Utf8String record in _records )
                    writer.WriteRecord(record);

                int blockPadding = _blockSize % recordSize;
                int totalPadding = (int)(stream.Length / _blockSize) * blockPadding;

                Assert.AreEqual(_records.Count, writer.RecordsWritten);
                Assert.AreEqual(_records.Count * recordSize, writer.OutputBytes);
                Assert.AreEqual(writer.OutputBytes + totalPadding, writer.BytesWritten);
                Assert.AreEqual(writer.BytesWritten, stream.Length);
            }

            int recordIndex = 0;
            DfsFile file = _dfsClient.NameServer.GetFileInfo(fileName);
            int blocks = file.Blocks.Count;
            int totalRecordsRead = 0;
            for( int block = 0; block < blocks; ++block )
            {
                int offset = block * _blockSize;
                int size = Math.Min((int)(file.Size - offset), _blockSize);
                using( DfsInputStream stream = _dfsClient.OpenFile(fileName) )
                using( LineRecordReader reader = new LineRecordReader(stream, block * _blockSize, size, true) )
                {
                    foreach( Utf8String record in reader.EnumerateRecords() )
                    {
                        Assert.AreEqual(_records[recordIndex], record);
                        ++recordIndex;
                    }

                    totalRecordsRead += reader.RecordsRead;
                    int recordCount = size / recordSize;
                    Assert.AreEqual(recordCount, reader.RecordsRead);
                    Assert.AreEqual(recordCount * recordSize, reader.InputBytes);
                    Assert.GreaterOrEqual(reader.BytesRead, recordCount * recordSize);
                    Assert.AreEqual(size, reader.BytesRead);
                    Assert.AreEqual(1, stream.BlocksRead);
                }
            }

            Assert.AreEqual(_records.Count, totalRecordsRead);
        }

        [Test]
        public void TestBinaryRecordReaderRecordsDoNotCrossBoundary()
        {
            const string fileName = "/binaryboundary";
            int recordSize = _records[0].ByteLength + 2; // BinaryRecordWriter writes string length which will take 2 bytes.
            using( DfsOutputStream stream = _dfsClient.CreateFile(fileName, 0, 0, RecordStreamOptions.DoNotCrossBoundary) )
            using( BinaryRecordWriter<Utf8String> writer = new BinaryRecordWriter<Utf8String>(stream) )
            {
                foreach( Utf8String record in _records )
                    writer.WriteRecord(record);

                int blockPadding = _blockSize % recordSize;
                int totalPadding = (int)(stream.Length / _blockSize) * blockPadding;

                Assert.AreEqual(_records.Count, writer.RecordsWritten);
                Assert.AreEqual(_records.Count * recordSize, writer.OutputBytes);
                Assert.AreEqual(writer.OutputBytes + totalPadding, writer.BytesWritten);
                Assert.AreEqual(writer.BytesWritten, stream.Length);
            }

            int recordIndex = 0;
            DfsFile file = _dfsClient.NameServer.GetFileInfo(fileName);
            int blocks = file.Blocks.Count;
            int totalRecordsRead = 0;
            for( int block = 0; block < blocks; ++block )
            {
                int offset = block * _blockSize;
                int size = Math.Min((int)(file.Size - offset), _blockSize);
                using( DfsInputStream stream = _dfsClient.OpenFile(fileName) )
                using( BinaryRecordReader<Utf8String> reader = new BinaryRecordReader<Utf8String>(stream, block * _blockSize, size, true) )
                {
                    foreach( Utf8String record in reader.EnumerateRecords() )
                    {
                        Assert.AreEqual(_records[recordIndex], record);
                        ++recordIndex;
                    }

                    totalRecordsRead += reader.RecordsRead;
                    int recordCount = size / recordSize;
                    Assert.AreEqual(recordCount, reader.RecordsRead);
                    Assert.AreEqual(recordCount * recordSize, reader.InputBytes);
                    Assert.GreaterOrEqual(reader.BytesRead, recordCount * recordSize);
                    Assert.AreEqual(size, reader.BytesRead);
                    Assert.AreEqual(1, stream.BlocksRead);
                }
            }

            Assert.AreEqual(_records.Count, totalRecordsRead);
        }
    }
}
