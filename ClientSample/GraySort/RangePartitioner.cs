using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Dfs;

namespace ClientSample.GraySort
{
    public sealed class RangePartitioner : Configurable, IPartitioner<GenSortRecord>
    {
        #region Nested types

        private abstract class TrieNode
        {
            public TrieNode(int depth)
            {
                Depth = depth;
            }

            public int Depth { get; private set; }

            public abstract int GetPartition(string value);
        }

        private sealed class InnerTrieNode : TrieNode
        {
            private TrieNode[] _children = new TrieNode[128]; // GenSort uses 7-bit ASCII so 128 elements is enough.

            public InnerTrieNode(int depth)
                : base(depth)
            {
            }

            public TrieNode this[int index]
            {
                get { return _children[index]; }
                set { _children[index] = value; }
            }

            public override int GetPartition(string value)
            {
                return _children[(int)value[Depth]].GetPartition(value);
            }
        }

        private sealed class LeafTrieNode : TrieNode
        {
            private string[] _splitPoints;
            private int _begin;
            private int _end;
            private StringComparer _comparer = StringComparer.Ordinal;

            public LeafTrieNode(int depth, string[] splitPoints, int begin, int end)
                : base(depth)
            {
                _splitPoints = splitPoints;
                _begin = begin;
                _end = end;
            }

            public override int GetPartition(string value)
            {
                for( int x = _begin; x < _end; ++x )
                {
                    if( _comparer.Compare(value, _splitPoints[x]) < 0 )
                        return x;
                }
                return _end;
            }

            public override string ToString()
            {
                StringBuilder result = new StringBuilder();
                for( int x = _begin; x < _end; ++x )
                {
                    result.Append(_splitPoints[x]);
                    result.Append(";");
                }
                return result.ToString();
            }
        }

        #endregion

        private TrieNode _trie;
        private string[] _splitPoints;

        #region IPartitioner<GenSortRecord> Members

        public int Partitions { get; set; }

        public int GetPartition(GenSortRecord value)
        {
            if( value == null )
                throw new ArgumentNullException("value");
            if( _trie == null )
            {
                ReadPartitionFile();
                _trie = BuildTrie(0, _splitPoints.Length, string.Empty, 2);
            }

            return _trie.GetPartition(value.Key);
        }

        #endregion

        public static void CreatePartitionFile(DfsClient dfsClient, string partitionFileName, TaskDfsInput[] inputs, int partitions, int sampleSize)
        {
            int samples = Math.Min(10, inputs.Length);
            int recordsPerSample = sampleSize / samples;
            int sampleStep = inputs.Length / samples;

            List<string> sampleData = new List<string>(sampleSize);

            for( int sample = 0; sample < samples; ++sample )
            {
                using( RecordReader<GenSortRecord> reader = inputs[sample * sampleStep].CreateRecordReader<GenSortRecord>(dfsClient, null) )
                {
                    int records = 0;
                    GenSortRecord record;
                    while( records++ < recordsPerSample && reader.ReadRecord(out record) )
                    {
                        sampleData.Add(record.Key);
                    }
                }
            }

            sampleData.Sort(StringComparer.Ordinal);

            dfsClient.NameServer.Delete(partitionFileName, false);

            float stepSize = sampleData.Count / (float)partitions;

            using( DfsOutputStream stream = dfsClient.CreateFile(partitionFileName) )
            using( BinaryRecordWriter<StringWritable> writer = new BinaryRecordWriter<StringWritable>(stream) )
            {
                for( int x = 1; x < partitions; ++x )
                {
                    writer.WriteRecord(sampleData[(int)Math.Round(x * stepSize)]);
                }
            }
        }

        private void ReadPartitionFile()
        {
            List<string> splitPoints = new List<string>();
            string partitionFileName = JobConfiguration.JobSettings["partitionFile"];
            DfsClient dfsClient = new DfsClient(DfsConfiguration);
            using( DfsInputStream stream = dfsClient.OpenFile(partitionFileName) )
            using( BinaryRecordReader<StringWritable> reader = new BinaryRecordReader<StringWritable>(stream) )
            {
                foreach( StringWritable record in reader.EnumerateRecords() )
                {
                    splitPoints.Add(record.Value);
                }
            }
            if( splitPoints.Count != Partitions - 1 )
                throw new InvalidOperationException("The partition file is invalid.");
            _splitPoints = splitPoints.ToArray();
        }

        private TrieNode BuildTrie(int begin, int end, string prefix, int maxDepth)
        {
            StringComparer comparer = StringComparer.Ordinal;
            int depth = prefix.Length;
            if( depth >= maxDepth || begin == end )
                return new LeafTrieNode(depth, _splitPoints, begin, end);

            InnerTrieNode result = new InnerTrieNode(depth);
            int current = begin;
            for( int x = 0; x < 128; ++x )
            {
                string newPrefix = prefix + (char)(x+1);
                begin = current;
                while( current < end && comparer.Compare(_splitPoints[current], newPrefix) < 0 )
                {
                    ++current;
                }
                result[x] = BuildTrie(begin, current, prefix + (char)x, maxDepth);
            }
            return result;
        }
    }
}
