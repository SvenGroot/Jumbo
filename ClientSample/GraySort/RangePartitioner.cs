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

            public abstract int GetPartition(byte[] key);
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

            public override int GetPartition(byte[] key)
            {
                return _children[key[Depth]].GetPartition(key);
            }
        }

        private sealed class LeafTrieNode : TrieNode
        {
            private byte[][] _splitPoints;
            private int _begin;
            private int _end;

            public LeafTrieNode(int depth, byte[][] splitPoints, int begin, int end)
                : base(depth)
            {
                _splitPoints = splitPoints;
                _begin = begin;
                _end = end;
            }

            public override int GetPartition(byte[] key)
            {
                for( int x = _begin; x < _end; ++x )
                {
                    if( GenSortRecord.CompareKeys(key, _splitPoints[x]) < 0 )
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
        private byte[][] _splitPoints;

        #region IPartitioner<GenSortRecord> Members

        public int Partitions { get; set; }

        public int GetPartition(GenSortRecord value)
        {
            if( value == null )
                throw new ArgumentNullException("value");
            if( _trie == null )
            {
                ReadPartitionFile();
                _trie = BuildTrie(0, _splitPoints.Length, new byte[] {}, 2);
            }

            return _trie.GetPartition(value.RecordBuffer);
        }

        #endregion

        public static void CreatePartitionFile(DfsClient dfsClient, string partitionFileName, TaskDfsInput[] inputs, int partitions, int sampleSize)
        {
            int samples = Math.Min(10, inputs.Length);
            int recordsPerSample = sampleSize / samples;
            int sampleStep = inputs.Length / samples;

            List<byte[]> sampleData = new List<byte[]>(sampleSize);

            for( int sample = 0; sample < samples; ++sample )
            {
                using( RecordReader<GenSortRecord> reader = inputs[sample * sampleStep].CreateRecordReader<GenSortRecord>(dfsClient, null) )
                {
                    int records = 0;
                    GenSortRecord record;
                    while( records++ < recordsPerSample && reader.ReadRecord(out record) )
                    {
                        sampleData.Add(record.ExtractKeyBytes());
                    }
                }
            }

            sampleData.Sort(GenSortRecord.CompareKeys);

            dfsClient.NameServer.Delete(partitionFileName, false);

            float stepSize = sampleData.Count / (float)partitions;

            using( DfsOutputStream stream = dfsClient.CreateFile(partitionFileName) )
            {
                for( int x = 1; x < partitions; ++x )
                {
                    stream.Write(sampleData[(int)Math.Round(x * stepSize)], 0, GenSortRecord.KeySize);
                }
            }
        }

        private void ReadPartitionFile()
        {
            List<byte[]> splitPoints = new List<byte[]>();
            string partitionFileName = TaskAttemptConfiguration.JobConfiguration.JobSettings["partitionFile"];
            DfsClient dfsClient = new DfsClient(DfsConfiguration);
            using( DfsInputStream stream = dfsClient.OpenFile(partitionFileName) )
            {
                int bytesRead;
                do
                {
                    byte[] key = new byte[GenSortRecord.KeySize];
                    bytesRead = stream.Read(key, 0, GenSortRecord.KeySize);
                    if( bytesRead == GenSortRecord.KeySize )
                    {
                        splitPoints.Add(key);
                    }
                } while( bytesRead == GenSortRecord.KeySize );
            }
            if( splitPoints.Count != Partitions - 1 )
                throw new InvalidOperationException("The partition file is invalid.");
            _splitPoints = splitPoints.ToArray();
        }

        private TrieNode BuildTrie(int begin, int end, byte[] prefix, int maxDepth)
        {
            int depth = prefix.Length;
            if( depth >= maxDepth || begin == end )
                return new LeafTrieNode(depth, _splitPoints, begin, end);

            InnerTrieNode result = new InnerTrieNode(depth);
            int current = begin;
            for( int x = 0; x < 128; ++x )
            {
                byte[] newPrefix = new byte[depth + 1];
                prefix.CopyTo(newPrefix, 0);
                newPrefix[depth] = (byte)(x + 1);
                begin = current;
                while( current < end && GenSortRecord.ComparePartialKeys(_splitPoints[current], newPrefix) < 0 )
                {
                    ++current;
                }
                newPrefix[depth] = (byte)x;
                result[x] = BuildTrie(begin, current, newPrefix, maxDepth);
            }
            return result;
        }
    }
}
