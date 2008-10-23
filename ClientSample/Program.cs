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
using System.Diagnostics;

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
            Console.WriteLine("Press any key to start");
            Console.ReadKey();
            Stopwatch sw = new Stopwatch();
            sw.Start();
            //System.Threading.Thread.Sleep(3000); // wait for data server to report to name server
            //BlockAssignment b = nameServer.CreateFile("/test");
            //WriteBlock(b);
            //nameServer.CloseFile("/test");

            //Tkl.Jumbo.Dfs.File file = nameServer.GetFileInfo("/myfile");
            //string[] servers = nameServer.GetDataServersForBlock(file.Blocks[0]);
            //ReadBlock(file, servers);

            WriteFile(args, nameServer);

            sw.Stop();
            Console.WriteLine(sw.Elapsed);
            //ReadFile(nameServer);

            Console.WriteLine("Done, press any key to exit");

            Console.ReadKey();
        }

        private static void WriteFile(string[] args, INameServerClientProtocol nameServer)
        {
            nameServer.Delete("/myfile", false);

            using( FileStream input = System.IO.File.OpenRead(args[0]) )
            using( DfsOutputStream stream = new DfsOutputStream(nameServer, "/myfile") )
            {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while( (bytesRead = input.Read(buffer, 0, buffer.Length)) > 0 )
                {
                    stream.Write(buffer, 0, bytesRead);
                }
            }
        }

        private static void ReadFile(INameServerClientProtocol nameServer)
        {
            using( FileStream output = System.IO.File.Create("test.dat") )
            using( DfsInputStream input = new DfsInputStream(nameServer, "/myfile") )
            {
                byte[] buffer = new byte[100000];
                input.Position = 100000;
                input.Read(buffer, 0, buffer.Length);
                output.Write(buffer, 0, buffer.Length);
            }
        }

        private static void ReadBlock(Tkl.Jumbo.Dfs.File file, string[] servers)
        {
            using( TcpClient client = new TcpClient(servers[0], 9001) )
            {
                DataServerClientProtocolReadHeader header = new DataServerClientProtocolReadHeader();
                header.BlockID = file.Blocks[0];
                header.Offset = 100000;
                header.Size = 100000;

                int receivedSize = 0;
                using( NetworkStream stream = client.GetStream() )
                {
                    BinaryFormatter formatter = new BinaryFormatter();
                    formatter.Serialize(stream, header);

                    using( BinaryReader reader = new BinaryReader(stream) )
                    using( FileStream result = System.IO.File.Create("test.txt") )
                    {
                        DataServerClientProtocolResult status = (DataServerClientProtocolResult)reader.ReadInt32();
                        if( status != DataServerClientProtocolResult.Ok )
                            throw new Exception("AARGH!");
                        int offset = reader.ReadInt32();

                        Packet packet = new Packet();
                        while( !packet.IsLastPacket )
                        {
                            status = (DataServerClientProtocolResult)reader.ReadInt32();
                            if( status != DataServerClientProtocolResult.Ok )
                                throw new Exception("AARGH!");
                            packet.Read(reader, false);

                            receivedSize += packet.Size;

                            packet.WriteDataOnly(result);
                        }

                    }
                }
                Console.WriteLine(receivedSize);
            }
        }

        private static void WriteBlock(BlockAssignment b)
        {
            //using( TcpClient client = new TcpClient(b.DataServers[0].HostName, 9001) )
            //{
            //    DataServerClientProtocolWriteHeader header = new DataServerClientProtocolWriteHeader();
            //    header.BlockID = b.BlockID;
            //    header.DataServers = null;
            //    int size = 10000000;

            //    using( NetworkStream stream = client.GetStream() )
            //    {
            //        BinaryFormatter formatter = new BinaryFormatter();
            //        formatter.Serialize(stream, header);

            //        using( BinaryWriter writer = new BinaryWriter(stream) )
            //        {
            //            Random rnd = new Random();
            //            int packetSize = 64 * 1024;
            //            for( int sizeRemaining = size; sizeRemaining > 0; sizeRemaining -= packetSize )
            //            {

            //                byte[] buffer = new byte[Math.Min(sizeRemaining, packetSize)];
            //                for( int x = 0; x < buffer.Length; ++x )
            //                {
            //                    buffer[x] = (byte)rnd.Next('a', 'z');
            //                }
            //                Crc32 crc = new Crc32();
            //                crc.Update(buffer);
            //                writer.Write((uint)crc.Value);
            //                writer.Write(buffer.Length);
            //                writer.Write(!(sizeRemaining - packetSize > 0));
            //                writer.Write(buffer);
            //            }
            //        }
            //    }
            //}
        }
    }
}
