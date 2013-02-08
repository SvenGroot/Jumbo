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
using System.Diagnostics;
using Tkl.Jumbo.Jet;

namespace TaskServerApplication
{
    /// <summary>
    /// A simple file server used by the file channel.
    /// </summary>
    /// <remarks>
    /// The protocol for this is very simple:
    /// - Request port number from the TaskServer.
    /// - Connect.
    /// - Send job ID (byte[])
    /// - Send true if single file output, otherwise false (boolean)
    /// - Send partition count (int32)
    /// - Send partitions (int32[])
    /// - Send task attempt ID count (int32)
    /// - Send task attempt IDs (string[])
    /// - If multi file output
    ///   - Send output stage ID (string)
    /// - For each task.
    ///   - For each partition
    ///     - If a failure occurs, the server writes -1 (int64).
    ///     - Server writes partition size (int64, may be 0)
    ///     - If multi file output
    ///       - Server writes uncompressed file size; if there's no compression this will equal the file size (int64)
    ///     - Server writes partition data
    /// </remarks>
    class FileChannelServer : TcpServer
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileChannelServer));
        private readonly TaskServer _taskServer;
        private readonly PartitionFileIndexCache _indexCache;
        private readonly int _bufferSize;

        public FileChannelServer(TaskServer taskServer, IPAddress[] localAddresses, int port, int maxConnections, int maxCacheSize)
            : base(localAddresses, port, maxConnections)
        {
            if( taskServer == null )
                throw new ArgumentNullException("taskServer");
            _taskServer = taskServer;
            _indexCache = new PartitionFileIndexCache(maxCacheSize);
            _bufferSize = (int)taskServer.Configuration.FileChannel.ReadBufferSize.Value;
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
                        Guid jobId = new Guid(guidBytes);
                        bool singleFileOutput = reader.ReadBoolean(); // Is the output a single indexed partition file?

                        int partitionCount = reader.ReadInt32();
                        int[] partitions = new int[partitionCount];
                        for( int x = 0; x < partitionCount; ++x )
                            partitions[x] = reader.ReadInt32();

                        int taskCount = reader.ReadInt32();
                        string[] tasks = new string[taskCount];
                        for( int x = 0; x < taskCount; ++x )
                        {
                            tasks[x] = reader.ReadString();
                        }

                        Stopwatch sw = _log.IsDebugEnabled ? Stopwatch.StartNew() : null;
                        if( singleFileOutput )
                        {
                            SendSingleFileOutput(writer, jobId, partitions, tasks);
                        }
                        else
                        {
                            string outputStageId = reader.ReadString();

                            string jobDirectory = _taskServer.GetJobDirectory(jobId);
                            foreach( string task in tasks )
                            {
                                foreach( int partition in partitions )
                                {
                                    string outputFile = FileOutputChannel.CreateChannelFileName(task, TaskId.CreateTaskIdString(outputStageId, partition));
                                    SendFile(writer, jobId, jobDirectory, outputFile);
                                }
                            }
                        }
                        if( _log.IsDebugEnabled )
                        {
                            sw.Stop();
                            _log.DebugFormat("Sent tasks {0} partitions {1} to client {2} in {3}ms", tasks.ToDelimitedString(","), partitions.ToDelimitedString(","), client.Client.RemoteEndPoint, sw.ElapsedMilliseconds);
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

        private void SendFile(BinaryWriter writer, Guid jobId, string jobDirectory, string file)
        {
            string path = Path.Combine(jobDirectory, file);
            //if( File.Exists(path) )
            //{
                long uncompressedSize = _taskServer.GetUncompressedTemporaryFileSize(jobId, file);

                using( FileStream fileStream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, _bufferSize) )
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

        private void SendSingleFileOutput(BinaryWriter writer, Guid jobId, int[] partitions, string[] tasks)
        {
            string dir = _taskServer.GetJobDirectory(jobId);
            foreach( string task in tasks )
            {
                string outputFile = FileOutputChannel.CreateChannelFileName(task, null);
                string path = Path.Combine(dir, outputFile);
                PartitionFileIndex index = _indexCache.GetIndex(path);
                using( FileStream stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, _bufferSize) )
                {
                    foreach( int partition in partitions )
                    {
                        IEnumerable<PartitionFileIndexEntry> entries = index.GetEntriesForPartition(partition);
                        if( entries == null )
                            writer.Write(0L);
                        else
                        {
                            int segmentCount = entries.Count();
                            long totalSize = entries.Sum(e => e.Count) + sizeof(long) * segmentCount;
                            writer.Write(totalSize);
                            writer.Write(segmentCount);
                            // No need for compressed size because compression is not supported for partition files currently.
                            foreach( PartitionFileIndexEntry entry in entries )
                            {
                                writer.Write(entry.Count);
                                stream.Seek(entry.Offset, SeekOrigin.Begin);
                                stream.CopySize(writer.BaseStream, entry.Count, 65536);
                            }
                        }
                    }
                }
            }
        }
    }
}
