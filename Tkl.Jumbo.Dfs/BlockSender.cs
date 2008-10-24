using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using System.Diagnostics;

namespace Tkl.Jumbo.Dfs
{
    public class BlockSender
    {
        private readonly Queue<Packet> _packetsToSend = new Queue<Packet>();
        private int _requiredConfirmations;
        private int _receivedConfirmations;
        private AutoResetEvent _packetsToSendEvent = new AutoResetEvent(false);
        private AutoResetEvent _requiredConfirmationsEvent = new AutoResetEvent(false);
        private volatile DataServerClientProtocolResult _lastResult = DataServerClientProtocolResult.Ok;
        private Thread _sendPacketsThread;
        private Thread _resultReaderThread;
        private volatile bool _finished;
        private bool _hasLastPacket;
        private readonly Guid _blockID;
        private readonly ServerAddress[] _dataServers;

        public BlockSender(BlockAssignment block)
        {
            if( block == null )
                throw new ArgumentNullException("block");

            _blockID = block.BlockID;
            _dataServers = block.DataServers.ToArray();
            _sendPacketsThread = new Thread(SendPacketsThread) { Name = "SendPackets" };
            _sendPacketsThread.Start();
        }

        // Note: if resultWriter is null, it will NOT send an ok for the last packet!
        public BlockSender(Guid blockID, ServerAddress[] dataServers)
        {
            if( dataServers == null )
                throw new ArgumentNullException("dataServers");

            _blockID = blockID;
            _dataServers = dataServers;
            _sendPacketsThread = new Thread(SendPacketsThread) { Name = "SendPackets" };
            _sendPacketsThread.Start();
        }

        public DataServerClientProtocolResult LastResult
        {
            get { return _lastResult; }
        }

        public void AddPacket(Packet packet)
        {
            if( packet == null )
                throw new ArgumentNullException("packet");


            lock( _packetsToSend )
            {
                _packetsToSend.Enqueue(packet);
            }
            if( packet.IsLastPacket )
            {
                _hasLastPacket = true;
            }
            _packetsToSendEvent.Set();
        }

        public void WaitForConfirmations()
        {
            if( !_hasLastPacket )
                throw new InvalidOperationException("You cannot call WaitForConfirmations until the last packet has been submitted.");
            _sendPacketsThread.Join();
        }

        public void ThrowIfErrorOccurred()
        {
            if( _lastResult != DataServerClientProtocolResult.Ok )
                throw new DfsException("There was an error sending a packet to the server.");
        }

        public void ForwardConfirmations(BinaryWriter writer)
        {
            if( _lastResult != DataServerClientProtocolResult.Ok )
            {
                writer.Write((int)_lastResult);
            }
            else
            {
                while( _receivedConfirmations > 0 )
                {
                    writer.Write((int)DataServerClientProtocolResult.Ok);
                    Interlocked.Decrement(ref _receivedConfirmations);
                }
            }
        }

        private void SendPacketsThread()
        {
            // TODO: error handling.
            ServerAddress server = _dataServers[0];
            using( TcpClient client = new TcpClient(server.HostName, server.Port) )
            using( NetworkStream stream = client.GetStream() )
            using( BinaryWriter writer = new BinaryWriter(stream) )
            using( BinaryReader reader = new BinaryReader(stream) )
            {
                _resultReaderThread = new Thread((obj) => ResultReaderThread((BinaryReader)obj));
                _resultReaderThread.Name = "ResultReader";
                _resultReaderThread.Start(reader);
                DataServerClientProtocolWriteHeader header = new DataServerClientProtocolWriteHeader();
                header.BlockID = _blockID;
                header.DataServers = _dataServers;

                BinaryFormatter formatter = new BinaryFormatter();
                formatter.Serialize(stream, header);

                Interlocked.Increment(ref _requiredConfirmations);
                _requiredConfirmationsEvent.Set();

                Packet packet = null;
                int prevTime = Environment.TickCount;
                while( (packet == null || !packet.IsLastPacket) && _lastResult == DataServerClientProtocolResult.Ok )
                {
                    int count;
                    lock( _packetsToSend )
                    {
                        count = _packetsToSend.Count;
                    }
                    if( count == 0 )
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
                        int newValue = Interlocked.Increment(ref _requiredConfirmations);

                        if( packet.IsLastPacket )
                        {
                            _finished = true;
                        }
                        packet.Write(writer, false);
                        _requiredConfirmationsEvent.Set();
                    }
                }

                // We must wait for the result reader thread to finish; it's using the network connection
                // so we can't close that.
                _resultReaderThread.Join();
            }
            Debug.WriteLine("Sending finished!");
        }

        private void ResultReaderThread(BinaryReader reader)
        {
            // TODO: Timeouts & error handling
            while( !(_finished && _requiredConfirmations == 0) && _lastResult == DataServerClientProtocolResult.Ok )
            {
                if( _requiredConfirmations == 0 )
                {
                    _requiredConfirmationsEvent.WaitOne();
                }

                if( _requiredConfirmations > 0 )
                {
                    DataServerClientProtocolResult result = (DataServerClientProtocolResult)reader.ReadInt32();
                    if( result != DataServerClientProtocolResult.Ok )
                    {
                        _lastResult = result;
                        break;
                    }
                    Interlocked.Decrement(ref _requiredConfirmations);
                    Interlocked.Increment(ref _receivedConfirmations);
                }
            }
            Debug.WriteLine("Receiving finished!");
        }
    }
}
