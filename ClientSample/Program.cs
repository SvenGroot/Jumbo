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
            nameServer.CreateFile("/test");
            BlockAssignment b = nameServer.AppendBlock("/test");

            using( TcpClient client = new TcpClient(b.DataServers[0], 9001) )
            {
                DataServerClientProtocolHeader header = new DataServerClientProtocolHeader();
                header.Command = DataServerCommand.WriteBlock;
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
            Console.ReadKey();
        }
    }
}
