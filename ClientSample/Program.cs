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
using Tkl.Jumbo.IO;

namespace ClientSample
{
    public class MyTask : ITask<string>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(MyTask));

        #region ITask Members

        public void Run(RecordReader<string> input)
        {
            _log.Info("Running");
            int lines = 0;
            string line;
            while( input.ReadRecord(out line) )
            {
                ++lines;
            }
            _log.Info(lines);
            _log.Info("Done");
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

            //StartJob(dfsClient, jobServer);

            using( MemoryStream stream = new MemoryStream() )
            using( BinaryRecordWriter<KeyValuePairWritable<StringWritable, Int32Writable>> writer = new BinaryRecordWriter<KeyValuePairWritable<StringWritable,Int32Writable>>(stream) )
            {
                KeyValuePairWritable<StringWritable, Int32Writable> pair = new KeyValuePairWritable<StringWritable, Int32Writable>();
                pair.Value = new KeyValuePair<StringWritable, Int32Writable>("hello", 5);
                writer.WriteRecord(pair);
                pair.Value = new KeyValuePair<StringWritable, Int32Writable>("bye", 42);
                writer.WriteRecord(pair);

                stream.Position = 0;
                using( BinaryRecordReader<KeyValuePairWritable<StringWritable, Int32Writable>> reader = new BinaryRecordReader<KeyValuePairWritable<StringWritable, Int32Writable>>(stream) )
                {
                    while( reader.ReadRecord(out pair) )
                    {
                        Console.WriteLine(pair);
                    }
                }
            }


            sw.Stop();
            Console.WriteLine(sw.Elapsed);

            Console.WriteLine("Done, press any key to exit");

            Console.ReadKey();
        }

        private static void StartJob(DfsClient dfsClient, IJobServerClientProtocol jobServer)
        {
            Tkl.Jumbo.Dfs.File file = dfsClient.NameServer.GetFileInfo("/test.txt");
            int blockSize = dfsClient.NameServer.BlockSize;

            JobConfiguration config = new JobConfiguration();
            config.AssemblyFileName = "ClientSample.exe";
            config.Tasks = new List<TaskConfiguration>() { 
                new TaskConfiguration() { 
                    TaskID = "Task1", 
                    TypeName = "ClientSample.MyTask" ,
                    DfsInput = new TaskDfsInput() {
                        Path = "/test.txt",
                        Offset = 0,
                        Size = blockSize,
                        RecordReaderType = typeof(LineRecordReader).AssemblyQualifiedName
                    }
                }, 
                new TaskConfiguration() { 
                    TaskID = "Task2", 
                    TypeName = "ClientSample.MyTask",
                    DfsInput = new TaskDfsInput() {
                        Path = "/test.txt",
                        Offset = blockSize,
                        Size = file.Size - blockSize,
                        RecordReaderType = typeof(LineRecordReader).AssemblyQualifiedName
                    }
                } 
            };

            using( FileStream stream = System.IO.File.Create("job.xml") )
            {
                config.SaveXml(stream);
            }


            Job job = jobServer.CreateJob();
            Console.WriteLine(job.JobID);
            using( DfsOutputStream stream = dfsClient.CreateFile(job.JobConfigurationFilePath) )
            {
                config.SaveXml(stream);
            }
            dfsClient.UploadFile(typeof(MyTask).Assembly.Location, DfsPath.Combine(job.Path, "ClientSample.exe"));

            jobServer.RunJob(job.JobID);
        }
    }
}
