using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;

namespace Tkl.Jumbo.Dfs
{
    public class BlockSender
    {
        private BlockAssignment _currentBlock;
        private readonly Queue<Packet> _packetsToSend = new Queue<Packet>();
        private int _requiredConfirmations;
        private bool _disposed;
        private AutoResetEvent _packetsToSendEvent = new AutoResetEvent(false);
        private volatile DataServerClientProtocolResult _lastResult = DataServerClientProtocolResult.Ok;
        private Thread _sendPacketsThread;
        private volatile bool _finished;

        public BlockSender(BlockAssignment block)
        {
            if( block == null )
                throw new ArgumentNullException("block");

            _currentBlock = block;
            _sendPacketsThread = new Thread(SendPacketsThread) { Name = "SendPackets" };
            _sendPacketsThread.Start();
        }

        public void AddPacket(Packet packet)
        {
            if( packet == null )
                throw new ArgumentNullException("packet");

            lock( _packetsToSend )
            {
                _packetsToSend.Enqueue(packet);
            }
            _packetsToSendEvent.Set();
        }

        private void SendPacketsThread()
        {
            ServerAddress server = _currentBlock.DataServers[0];
            using( TcpClient client = new TcpClient(server.HostName, server.Port) )
            using( NetworkStream stream = client.GetStream() )
            using( BinaryWriter writer = new BinaryWriter(stream) )
            using( BinaryReader reader = new BinaryReader(stream) )
            {
                DataServerClientProtocolWriteHeader header = new DataServerClientProtocolWriteHeader();
                header.BlockID = _currentBlock.BlockID;
                header.DataServers = _currentBlock.DataServers.ToArray();

                BinaryFormatter formatter = new BinaryFormatter();
                formatter.Serialize(stream, header);

                Interlocked.Increment(ref _requiredConfirmations);

                Packet packet = null;
                while( (packet == null || !packet.IsLastPacket) && _lastResult == DataServerClientProtocolResult.Ok )
                {
                    _packetsToSendEvent.WaitOne();

                    packet = null;
                    lock( _packetsToSend )
                    {
                        if( _packetsToSend.Count > 0 )
                        {
                            packet = _packetsToSend.Dequeue();
                        }
                    }
                    if( packet != null )
                    {
                        Interlocked.Increment(ref _requiredConfirmations);
                        packet.Write(writer, false);
                    }
                }
                _finished = true;
            }
        }

        private void ConfirmationReaderThread(BinaryReader reader)
        {
            while( !_finished )
            {
                // TODO: Stuff
            }
        }
    }
}
