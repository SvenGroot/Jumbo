// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;
using System.IO;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class MergeHelperTests
    {

        [Test]
        public void TestMerge()
        {
            TestMergeCore(5, 5, 100, 50, false);
        }

        [Test]
        public void TestMergeMultiplePasses()
        {
            TestMergeCore(12, 5, 100, 50, false);
        }

        [Test]
        public void TestMergeRaw()
        {
            TestMergeCore(5, 5, 100, 50, true);
        }

        [Test]
        public void TestMergeRawMultiplePasses()
        {
            TestMergeCore(12, 5, 100, 50, true);
        }

        private void TestMergeCore(int diskSegmentCount, int memorySegmentCount, int segmentItemCount, int segmentItemCountRandomization, bool rawComparer)
        {
            var diskSegmentData = GenerateSegmentData(diskSegmentCount, segmentItemCount, segmentItemCountRandomization);
            var diskSegments = GenerateSegments(diskSegmentData, false, rawComparer);
            var memorySegmentData = GenerateSegmentData(memorySegmentCount, segmentItemCount, segmentItemCountRandomization);
            var memorySegments = GenerateSegments(memorySegmentData, true, rawComparer);

            var expected = diskSegmentData.SelectMany(s => s).Concat(memorySegmentData.SelectMany(s => s)).OrderBy(s => s).ToList();

            var actual = MergeHelper<int>.Merge(diskSegments, memorySegments, 5, null, false, Utilities.TestOutputPath, CompressionType.None, 4096, true).Select(r => r.GetValue()).ToList();

            CollectionAssert.AreEqual(expected, actual);
        }

        private List<List<int>> GenerateSegmentData(int segmentCount, int itemCount, int itemCountRandomization)
        {
            List<List<int>> result = new List<List<int>>();
            Random rnd = new Random();
            for( int x = 0; x < segmentCount; ++x )
            {
                List<int> segment = Utilities.GenerateNumberData(itemCount + rnd.Next(itemCountRandomization), rnd);
                segment.Sort();
                result.Add(segment);
            }
            return result;
        }

        private List<RecordInput> GenerateSegments(List<List<int>> segments, bool isMemoryBased, bool serialize)
        {
            if( serialize )
            {
                List<RecordInput> result = new List<RecordInput>();
                foreach( List<int> segment in segments )
                {
                    MemoryStream stream = new MemoryStream();
                    BinaryWriter writer = new BinaryWriter(stream);
                    foreach( int value in segment )
                    {
                        WritableUtility.Write7BitEncodedInt32(writer, sizeof(int));
                        writer.Write(value);
                    }
                    stream.Position = 0;
                    result.Add(new StreamRecordInput(typeof(BinaryRecordReader<int>), stream, isMemoryBased, null, true, false));
                }
                return result;
            }
            else
                return segments.Select(s => (RecordInput)new ReaderRecordInput(new EnumerableRecordReader<int>(s), isMemoryBased)).ToList();
        }
    }
}
