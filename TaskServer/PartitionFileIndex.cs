// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Tkl.Jumbo.Jet.Channels;
using System.Reflection;
using System.IO;
using Tkl.Jumbo.IO;

namespace TaskServerApplication
{
    class PartitionFileIndex
    {
        private readonly ManualResetEvent _loadCompleteEvent = new ManualResetEvent(false);
        private Exception _loadException = null;
        private List<PartitionFileIndexEntry>[] _index;

        public PartitionFileIndex(string outputFilePath)
        {
            OutputFilePath = outputFilePath;
            ThreadPool.QueueUserWorkItem(LoadIndex, outputFilePath + ".index");
        }

        public string OutputFilePath { get; private set; }

        public IEnumerable<PartitionFileIndexEntry> GetEntriesForPartition(int partition)
        {
            _loadCompleteEvent.WaitOne();
            if( _loadException != null )
                throw new TargetInvocationException(_loadException);
            return _index[partition - 1];
        }

        private void LoadIndex(object state)
        {
            try
            {
                string indexFilePath = (string)state;
                using( FileStream stream = File.OpenRead(indexFilePath) )
                using( BinaryRecordReader<PartitionFileIndexEntry> reader = new BinaryRecordReader<PartitionFileIndexEntry>(stream, false) )
                {
                    foreach( PartitionFileIndexEntry entry in reader.EnumerateRecords() )
                    {
                        if( _index == null )
                            _index = new List<PartitionFileIndexEntry>[entry.Partition]; // First entry isn't a real entry but gives us the total number of partitions.
                        else
                        {
                            List<PartitionFileIndexEntry> partition = _index[entry.Partition];
                            if( partition == null )
                            {
                                partition = new List<PartitionFileIndexEntry>(1);
                                _index[entry.Partition] = partition;
                            }
                            partition.Add(entry);
                        }
                    }
                }
            }
            catch( Exception ex )
            {
                _loadException = ex;
            }
            _loadCompleteEvent.Set();
        }
    }
}
