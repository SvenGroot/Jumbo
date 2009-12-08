﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;
using System.Net;
using System.IO;
using System.Net.Sockets;

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
    /// - Send requested file name using BinaryWriter.Write(string)
    /// - Server writes a the compressed and uncompressed size of the file to the stream as two longs (Int64), or -1 if there's a failure.
    /// - Server writes file if it exists.
    /// </remarks>
    class FileChannelServer : TcpServer
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileChannelServer));
        private TaskServer _taskServer;

        public FileChannelServer(TaskServer taskServer, IPAddress[] localAddresses, int port, int maxConnections)
            : base(localAddresses, port, maxConnections)
        {
            if( taskServer == null )
                throw new ArgumentNullException("taskServer");
            _taskServer = taskServer;
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
    }
}
