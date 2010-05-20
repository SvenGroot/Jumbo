// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;
using System.Net;
using System.IO;
using System.Net.Sockets;
using Tkl.Jumbo.Jet.Channels;

namespace TaskServerApplication
{
    /// <summary>
    /// A simple file server used by the file channel.
    /// </summary>
    /// <remarks>
    /// The protocol for this is very simple:
    /// - Request port number from the TaskServer.
    /// - Connect.
    /// - Send job ID as byte array.
    /// - Send boolean true if single file output, otherwise false.
    /// - If single file output:
    ///   - Write output file name (string)
    ///   - Write partition count (int32)
    ///   - Write partitions (int32)
    ///   - For each partition
    ///     - Server returns size (int64, may be 0, compression not currently supported)
    ///     - Server returns partition data.
    /// - If multi file output
    ///   - Send number of files (int32)
    ///   - Send file names (string)
    ///   - For each file.
    ///     - Server writes a the compressed and uncompressed size of the file to the stream as two longs (Int64), or -1 if there's a failure.
    ///     - Server writes file if it exists.
    /// </remarks>
    class FileChannelServer : TcpServer
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileChannelServer));
        private readonly TaskServer _taskServer;
        private readonly PartitionFileIndexCache _indexCache;

        public FileChannelServer(TaskServer taskServer, IPAddress[] localAddresses, int port, int maxConnections, int maxCacheSize)
            : base(localAddresses, port, maxConnections)
        {
            if( taskServer == null )
                throw new ArgumentNullException("taskServer");
            _taskServer = taskServer;
            _indexCache = new PartitionFileIndexCache(maxCacheSize);
        }

        protected override void HandleConnection(System.Net.Sockets.TcpClient client)
        {
            try
            {
                using( NetworkStream stream = client.GetStream() )
                using( BinaryReader reader = new BinaryReader(stream) )
                using( BinaryWriter writer = new BinaryWriter(stream) )
                {
                    try
                    {
                        byte[] guidBytes = reader.ReadBytes(16);
                        Guid jobID = new Guid(guidBytes);
                        bool singleFileOutput = reader.ReadBoolean(); // Is the output a single indexed partition file?
                        if( singleFileOutput )
                        {
                            string fileName = reader.ReadString();
                            int partitionCount = reader.ReadInt32();
                            int[] partitions = new int[partitionCount];
                            for( int x = 0; x < partitionCount; ++x )
                                partitions[x] = reader.ReadInt32();

                            SendPartitionsFromSingleFileOutput(writer, jobID, fileName, partitions);
                        }
                        else
                        {
                            int fileCount = reader.ReadInt32();
                            string[] files = new string[fileCount];
                            for( int x = 0; x < fileCount; ++x )
                            {
                                files[x] = reader.ReadString();
                            }

                            _log.DebugFormat("Sending files {0} to {1}", files.ToDelimitedString(), client.Client.RemoteEndPoint);
                            foreach( string file in files )
                                SendFile(writer, jobID, file);
                            _log.DebugFormat("Sending files {0} complete.", files.ToDelimitedString());
                        }
                    }
                    catch( Exception )
                    {
                        try
                        {
                            writer.Write(-1L);
                        }
                        catch( Exception )
                        {
                        }
                        throw;
                    }
                }
            }
            catch( Exception ex )
            {
                _log.Error("An error occurred handling a client connection.", ex);
            }
        }

        private void SendFile(BinaryWriter writer, Guid jobID, string file)
        {
            string dir = _taskServer.GetJobDirectory(jobID);
            string path = Path.Combine(dir, file);
            //if( File.Exists(path) )
            //{
                long uncompressedSize = _taskServer.GetUncompressedTemporaryFileSize(jobID, file);

                using( FileStream fileStream = File.OpenRead(path) )
                {
                    writer.Write(fileStream.Length);
                    if( uncompressedSize == -1 )
                        writer.Write(fileStream.Length);
                    else
                        writer.Write(uncompressedSize);
                    fileStream.CopyTo(writer.BaseStream);
                }
            //}
            //else
            //{
            //    writer.Write(-1L);
            //}
        }

        private void SendPartitionsFromSingleFileOutput(BinaryWriter writer, Guid jobId, string file, IEnumerable<int> partitions)
        {
            string dir = _taskServer.GetJobDirectory(jobId);
            string path = Path.Combine(dir, file);
            PartitionFileIndex index = _indexCache.GetIndex(path);
            using( FileStream stream = File.OpenRead(path) )
            {
                foreach( int partition in partitions )
                {
                    IEnumerable<PartitionFileIndexEntry> entries = index.GetEntriesForPartition(partition);
                    if( entries == null )
                        writer.Write(0L);
                    else
                    {
                        long totalSize = entries.Sum(e => e.Count);
                        writer.Write(totalSize);
                        // No need for compressed size because compression is not supported for partition files currently.
                        foreach( PartitionFileIndexEntry entry in entries )
                        {
                            stream.Seek(entry.Offset, SeekOrigin.Begin);
                            stream.CopySize(writer.BaseStream, entry.Count, 65516);
                        }
                    }
                }
            }
        }
    }
}
