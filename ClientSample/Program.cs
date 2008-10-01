using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Runtime.Remoting;
using System.Net.Sockets;
using System.Net;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;

namespace ClientSample
{
    class Program
    {
        static void Main(string[] args)
        {
            RemotingConfiguration.Configure("ClientSample.exe.config", false);
            var types = RemotingConfiguration.GetRegisteredWellKnownClientTypes();
            INameServerClientProtocol nameServer = (INameServerClientProtocol)Activator.GetObject(types[0].ObjectType, types[0].ObjectUrl);
            //nameServer.CreateDirectory("/test/foo");
            //nameServer.CreateFile("/test/bar");
            //File f = nameServer.GetFileInfo("/test/bar");
            System.Threading.Thread.Sleep(3000); // wait for data server to report to name server
            //BlockAssignment b = nameServer.CreateFile("/test");
            //WriteBlock(b);
            //nameServer.CloseFile("/test");

            Tkl.Jumbo.Dfs.File file = nameServer.GetFileInfo("/test");
            string[] servers = nameServer.GetDataServersForBlock(file.Blocks[0]);
            using( TcpClient client = new TcpClient(servers[0], 9001) )
            {
                DataServerClientProtocolReadHeader header = new DataServerClientProtocolReadHeader();
                header.BlockID = file.Blocks[0];
                header.Offset = 100000;
                header.Size = 100000;

                using( NetworkStream stream = client.GetStream() )
                {
                    BinaryFormatter formatter = new BinaryFormatter();
                    formatter.Serialize(stream, header);
                    int _packetSize = 64 * 1024;

                    using( BinaryReader reader = new BinaryReader(stream) )
                    using( BinaryWriter writer = new BinaryWriter(System.IO.File.Create("test.txt")) )
                    {
                        int offset = reader.ReadInt32();
                        int size = reader.ReadInt32();

                        int sizeRemaining = size;
                        byte[] buffer = new byte[_packetSize];
                        Crc32 computedChecksum = new Crc32();
                        while( sizeRemaining > 0 )
                        {
                            uint checksum = reader.ReadUInt32();
                            computedChecksum.Reset();
                            int packetSize = Math.Min(sizeRemaining, _packetSize);
                            int bytesRead = 0;
                            while( bytesRead < packetSize )
                            {
                                bytesRead += reader.Read(buffer, bytesRead, packetSize - bytesRead);
                            }

                            computedChecksum.Update(buffer, 0, packetSize);
                            if( computedChecksum.Value != checksum )
                                throw new Exception(); // TODO: handle this properly

                            writer.Write(buffer, 0, packetSize);
                            sizeRemaining -= packetSize;
                        }

                    }
                }
            }
            Console.ReadKey();
        }

        private static void WriteBlock(BlockAssignment b)
        {
            using( TcpClient client = new TcpClient(b.DataServers[0], 9001) )
            {
                DataServerClientProtocolWriteHeader header = new DataServerClientProtocolWriteHeader();
                header.BlockID = b.BlockID;
                header.DataServers = null;
                header.DataSize = 10000000;

                using( NetworkStream stream = client.GetStream() )
                {
                    BinaryFormatter formatter = new BinaryFormatter();
                    formatter.Serialize(stream, header);

                    using( BinaryWriter writer = new BinaryWriter(stream) )
                    {
                        Random rnd = new Random();
                        int packetSize = 64 * 1024;
                        for( int sizeRemaining = header.DataSize; sizeRemaining > 0; sizeRemaining -= packetSize )
                        {

                            byte[] buffer = new byte[Math.Min(sizeRemaining, packetSize)];
                            for( int x = 0; x < buffer.Length; ++x )
                            {
                                buffer[x] = (byte)rnd.Next('a', 'z');
                            }
                            Crc32 crc = new Crc32();
                            crc.Update(buffer);
                            writer.Write((uint)crc.Value);
                            writer.Write(buffer);
                        }
                    }
                }
            }
        }
    }
}
