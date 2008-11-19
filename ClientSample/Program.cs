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
using Tkl.Jumbo.Jet.Channels;

namespace ClientSample
{
    public class MyTask : ITask<StringWritable, Int32Writable>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(MyTask));

        #region ITask Members

        public void Run(RecordReader<StringWritable> input, RecordWriter<Int32Writable> writer)
        {
            _log.Info("Running");
            int lines = 0;
            StringWritable line;
            while( input.ReadRecord(out line) )
            {
                ++lines;
            }
            _log.Info(lines);
            if( writer != null )
                writer.WriteRecord(lines);
            _log.Info("Done");
        }

        #endregion
    }

    public class MyTask2 : ITask<Int32Writable, Int32Writable>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(MyTask));

        #region ITask<Int32Writable,Int32Writable> Members

        public void Run(RecordReader<Int32Writable> input, RecordWriter<Int32Writable> output)
        {
            _log.InfoFormat("Running, input = {0}, output = {1}", input, output);
            int totalLines = 0;
            foreach( Int32Writable value in input.EnumerateRecords() )
            {
                totalLines += value.Value;
                _log.Info(value);
            }
            _log.InfoFormat("Total: {0}", totalLines);
            output.WriteRecord(totalLines);
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

            StartJob(dfsClient, jobServer);
            //dfsClient.NameServer.Move("/JumboJet/job_{57f5850e-7637-4d08-87eb-03b9cfef9a90}/Task3", "/foo.txt");

            sw.Stop();
            Console.WriteLine(sw.Elapsed);

            Console.WriteLine("Done, press any key to exit");

            Console.ReadKey();
        }

        private static void StartJob(DfsClient dfsClient, IJobServerClientProtocol jobServer)
        {
            const string fileName = "/large.txt";
            Tkl.Jumbo.Dfs.File file = dfsClient.NameServer.GetFileInfo(fileName);
            int blockSize = dfsClient.NameServer.BlockSize;

            JobConfiguration config = new JobConfiguration()
            {
                AssemblyFileName = "ClientSample.exe",
                Tasks = new List<TaskConfiguration>(),
                Channels = new List<ChannelConfiguration>()
            };

            string[] tasks = new string[file.Blocks.Count];
            for( int x = 0; x < file.Blocks.Count; ++x )
            {
                config.Tasks.Add(new TaskConfiguration()
                {
                    TaskID = "Task" + (x + 1).ToString(),
                    TypeName = typeof(MyTask).FullName,
                    DfsInput = new TaskDfsInput()
                    {
                        Path = fileName,
                        Block = x,
                        RecordReaderType = typeof(LineRecordReader).AssemblyQualifiedName
                    }
                });
                tasks[x] = "Task" + (x + 1).ToString();
            }

            config.Tasks.Add(new TaskConfiguration()
            {
                TaskID = "OutputTask",
                TypeName = typeof(MyTask2).FullName,
                DfsOutput = new TaskDfsOutput()
                {
                    Path = "/output/count.txt",
                    RecordWriterType = typeof(TextRecordWriter<Int32Writable>).AssemblyQualifiedName
                }
            });

            config.Channels.Add(new ChannelConfiguration()
            {
                ChannelType = ChannelType.File,
                InputTasks = tasks,
                OutputTaskID = "OutputTask"
            });

            using( FileStream stream = System.IO.File.Create("job.xml") )
            {
                config.SaveXml(stream);
            }

            dfsClient.NameServer.Delete("/output", true);
            dfsClient.NameServer.CreateDirectory("/output");
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
