﻿using System;
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
using Tkl.Jumbo.Jet;
using System.Xml.Serialization;
using System.Threading;

namespace ClientSample
{
    public class MyTask : ITask
    {
        #region ITask Members

        public void Run()
        {
            Console.WriteLine("Running");
            Thread.Sleep(5000);
            Console.WriteLine("Done");
        }

        #endregion
    }

    class Program
    {
        static void Main(string[] args)
        {
            DfsClient dfsClient = new DfsClient();
            IJobServerClientProtocol jobServer = JetClient.CreateJobServerClient();
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

            JobConfiguration config = new JobConfiguration();
            config.AssemblyFileName = "ClientSample.exe";
            config.Tasks = new List<TaskConfiguration>() { new TaskConfiguration() { TaskID = "Task1", TypeName = "ClientSample.MyTask" }, new TaskConfiguration() { TaskID = "Task2", TypeName = "ClientSample.MyTask" } };

            Job job = jobServer.CreateJob();
            using( DfsOutputStream stream = dfsClient.CreateFile(job.JobConfigurationFilePath) )
            {
                config.SaveXml(stream);
            }
            dfsClient.UploadFile(typeof(MyTask).Assembly.Location, DfsPath.Combine(job.Path, "ClientSample.exe"));

            jobServer.RunJob(job.JobID);

            //WriteFile(args, nameServer);

            //Tkl.Jumbo.Dfs.File file = nameServer.GetFileInfo("/myfile");
            //ServerAddress[] servers = nameServer.GetDataServersForBlock(file.Blocks[0]);
            //ReadBlock(file, servers, nameServer.BlockSize);

            //ReadFile(nameServer);

            sw.Stop();
            Console.WriteLine(sw.Elapsed);

            Console.WriteLine("Done, press any key to exit");

            //Console.ReadKey();
        }

        private static void WriteFile(string[] args, INameServerClientProtocol nameServer)
        {
            try
            {
                nameServer.Delete("/myfile", false);

                using( FileStream input = System.IO.File.OpenRead(args[0]) )
                using( DfsOutputStream stream = new DfsOutputStream(nameServer, "/myfile") )
                {
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    int time = 0;
                    do
                    {
                        int before = Environment.TickCount;
                        bytesRead = input.Read(buffer, 0, buffer.Length);
                        time += Environment.TickCount - before;
                        if( bytesRead > 0 )
                            stream.Write(buffer, 0, bytesRead);
                    } while( bytesRead > 0 );
                    Console.WriteLine("Read time ms: {0}", TimeSpan.FromMilliseconds(time));
                }
            }
            catch( InvalidOperationException ex )
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static void ReadFile(INameServerClientProtocol nameServer)
        {
            using( FileStream output = System.IO.File.Create("test.txt") )
            using( DfsInputStream input = new DfsInputStream(nameServer, "/myfile") )
            {
                byte[] buffer = new byte[4096];
                //input.Position = 100000;
                //input.Read(buffer, 0, buffer.Length);
                //output.Write(buffer, 0, buffer.Length);
                //input.Position = 500000;
                //input.Read(buffer, 0, buffer.Length);
                //output.Write(buffer, 0, buffer.Length);

                int readBytes;
                do
                {
                    readBytes = input.Read(buffer, 0, buffer.Length);
                    //output.Write(buffer, 0, readBytes);
                } while( readBytes > 0 );
            }
        }

        //private static void ReadBlock(Tkl.Jumbo.Dfs.File file, ServerAddress[] servers, int blockSize)
        //{
        //    using( TcpClient client = new TcpClient(servers[0].HostName, servers[0].Port) )
        //    {
        //        DataServerClientProtocolReadHeader header = new DataServerClientProtocolReadHeader();
        //        header.BlockID = file.Blocks[0];
        //        header.Offset = 0;
        //        header.Size = blockSize;

        //        int receivedSize = 0;
        //        using( NetworkStream stream = client.GetStream() )
        //        {
        //            BinaryFormatter formatter = new BinaryFormatter();
        //            formatter.Serialize(stream, header);

        //            using( BinaryReader reader = new BinaryReader(stream) )
        //            using( FileStream result = System.IO.File.Create("test.txt") )
        //            {
        //                DataServerClientProtocolResult status = (DataServerClientProtocolResult)reader.ReadInt32();
        //                if( status != DataServerClientProtocolResult.Ok )
        //                    throw new Exception("AARGH!");
        //                int offset = reader.ReadInt32();

        //                Packet packet = new Packet();
        //                while( !packet.IsLastPacket )
        //                {
        //                    status = (DataServerClientProtocolResult)reader.ReadInt32();
        //                    if( status != DataServerClientProtocolResult.Ok )
        //                        throw new Exception("AARGH!");
        //                    packet.Read(reader, false);

        //                    receivedSize += packet.Size;

        //                    packet.WriteDataOnly(result);
        //                }

        //            }
        //        }
        //        Console.WriteLine(receivedSize);
        //    }
        //}

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
