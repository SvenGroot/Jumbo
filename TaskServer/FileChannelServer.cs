using System;
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
    /// - Server writes a the size of the file to the stream as a long (Int64), or -1 if there's a failure.
    /// - Server writes file if it exists.
    /// </remarks>
    class FileChannelServer : TcpServer
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileChannelServer));
        private TaskServer _taskServer;

        public FileChannelServer(TaskServer taskServer, IPAddress localAddress, int port)
            : base(localAddress, port)
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
                        string file = reader.ReadString();

                        SendFile(writer, jobID, file);
                    }
                    catch( Exception )
                    {
                        writer.Write(-1L);
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
            if( File.Exists(path) )
            {
                using( FileStream fileStream = File.OpenRead(path) )
                {
                    writer.Write(fileStream.Length);
                    fileStream.CopyTo(writer.BaseStream);
                }
            }
            else
            {
                writer.Write(-1L);
            }
        }
    }
}
