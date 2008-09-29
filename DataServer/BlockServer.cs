using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Net.Sockets;
using System.Net;
using Tkl.Jumbo.Dfs;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;

namespace DataServer
{
    /// <summary>
    /// Provides a TCP server that clients can use to read and write blocks to the data server.
    /// </summary>
    class BlockServer
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(BlockServer));

        private const int _packetSize = 64 * 1024; // 64KB

        private AutoResetEvent _connectionEvent = new AutoResetEvent(false);
        private TcpListener _listener = new TcpListener(IPAddress.Any, 9001); // TODO: Get port from configuration
        private Thread _listenerThread;
        private DataServer _dataServer;

        public BlockServer(DataServer dataServer)
        {
            if( dataServer == null )
                throw new ArgumentNullException("dataServer");

            _dataServer = dataServer;
        }
        public void Run()
        {
            _listener.Start();
            _log.Info("TCP server started.");

            while( true )
            {
                WaitForConnections();
            }
        }

        public void RunAsync()
        {
            if( _listenerThread == null )
            {
                _listenerThread = new Thread(new ThreadStart(Run));
                _listenerThread.Name = "listener";
                _listenerThread.IsBackground = true;
                _listenerThread.Start();
            }
        }

        private void WaitForConnections()
        {
            _listener.BeginAcceptTcpClient(new AsyncCallback(AcceptTcpClientCallback), null);

            _connectionEvent.WaitOne();
        }

        private void AcceptTcpClientCallback(IAsyncResult ar)
        {
            _log.Info("Connection accepted.");
            _connectionEvent.Set();
            using( TcpClient client = _listener.EndAcceptTcpClient(ar) )
            {
                HandleConnection(client);
            }
        }

        private void HandleConnection(TcpClient client)
        {
            using( NetworkStream stream = client.GetStream() )
            {
                BinaryFormatter formatter = new BinaryFormatter();
                DataServerClientProtocolHeader header = (DataServerClientProtocolHeader)formatter.Deserialize(stream);

                if( header.Command == DataServerCommand.WriteBlock )
                {
                    if( header.DataSize > _dataServer.BlockSize )
                        throw new Exception(); // TODO: Handle this properly
                    WriteBlock(stream, header);
                }
            }
        }

        private void WriteBlock(NetworkStream stream, DataServerClientProtocolHeader header)
        {
            _log.InfoFormat("Block write command received: block {0}, size {1}.", header.BlockID, header.DataSize);
            int sizeRemaining = header.DataSize;

            // TODO: If something goes wrong, the block must be deleted.
            using( BinaryReader reader = new BinaryReader(stream) )
            using( FileStream blockFile = _dataServer.AddNewBlock(header.BlockID) )
            using( BinaryWriter writer = new BinaryWriter(blockFile) )
            {
                byte[] buffer = new byte[_packetSize];
                while( sizeRemaining > 0 )
                {
                    uint checksum = reader.ReadUInt32();
                    Crc32 computedChecksum = new Crc32();
                    int packetSize = Math.Min(sizeRemaining, _packetSize);
                    int bytesRead = 0;
                    while( bytesRead < packetSize )
                    {
                        bytesRead += reader.Read(buffer, bytesRead, packetSize - bytesRead);
                    }

                    computedChecksum.Update(buffer, 0, packetSize);
                    if( computedChecksum.Value != checksum )
                        throw new Exception(); // TODO: handle this properly

                    // TODO: Forward the packet to the other servers.

                    writer.Write(checksum);
                    writer.Write(buffer, 0, packetSize);
                    sizeRemaining -= packetSize;
                }
            }
            _log.InfoFormat("Writing block {0} complete.", header.BlockID);
            _dataServer.CompleteBlock(header.BlockID, header.DataSize);
        }
    }
}
