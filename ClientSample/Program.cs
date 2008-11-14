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
            //Console.ReadKey();
            Stopwatch sw = new Stopwatch();
            sw.Start();

            //byte[] buffer = new byte[4096];
            //using( Stream stream = System.IO.File.OpenRead("E:\\test.txt") ) //dfsClient.OpenFile("/test.txt") )
            //{
            //    while( stream.Read(buffer, 0, buffer.Length) > 0 )
            //    {
            //    }
            //}

            //using( Stream stream = System.IO.File.OpenRead(@"D:\jumbo\blocks\a79c8b34-5692-4f68-87ad-893b88318f3e") )
            //using( BinaryReader reader = new BinaryReader(stream) )
            //{
            //    Packet packet = new Packet();
            //    while( !packet.IsLastPacket )
            //        packet.Read(reader, true);
            //}

            //using( TcpClient client = new TcpClient("localhost", 9001) )
            //using( NetworkStream stream = client.GetStream() )
            //{
            //    DataServerClientProtocolReadHeader header = new DataServerClientProtocolReadHeader();
            //    header.BlockID = new Guid("a79c8b34-5692-4f68-87ad-893b88318f3e");
            //    header.Offset = 0;
            //    header.Size = -1;
            //    BinaryFormatter formatter = new BinaryFormatter();
            //    formatter.Serialize(stream, header);
            //    using( BinaryReader reader = new BinaryReader(stream) )
            //    {
            //        DataServerClientProtocolResult result = (DataServerClientProtocolResult)reader.ReadInt32();
            //        int offset = reader.ReadInt32();
            //        Packet packet = new Packet();
            //        while( !packet.IsLastPacket )
            //        {
            //            result = (DataServerClientProtocolResult)reader.ReadInt32();
            //            packet.Read(reader, false);
            //        }
            //    }
            //}

            //StartJob(dfsClient, jobServer);

            string line = null;
            using( Stream stream = dfsClient.OpenFile("/test.txt") )
            using( LineRecordReader reader = new LineRecordReader(stream) )
            {
                while( (line = reader.ReadRecord()) != null )
                {
                }
            }
            //Console.WriteLine(line);
            //string line = null;
            //using( StreamReader reader = new StreamReader("E:\\test.txt") )
            //{
            //    while( (line = reader.ReadLine()) != null )
            //    {
            //    }
            //}
            //Console.WriteLine(line);

            sw.Stop();
            Console.WriteLine(sw.Elapsed);

            Console.WriteLine("Done, press any key to exit");

            //Console.ReadKey();
        }

        private static void StartJob(DfsClient dfsClient, IJobServerClientProtocol jobServer)
        {
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
        }
    }
}
