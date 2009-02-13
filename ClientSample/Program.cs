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
    class Program
    {
        static void Main(string[] args)
        {
            if( args.Length < 2 || args.Length > 5 )
            {
                Console.WriteLine("Usage: ClientSample.exe <task> <inputfile> [aggregate task count] [outputpath] [profile options]");
                return;
            }

            string task = args[0];
            string input = args[1];
            int aggregateTaskCount = args.Length >= 3 ? Convert.ToInt32(args[2]) : 1;
            string output = args.Length >= 4 ? args[3] : "/output";
            string profileOptions = args.Length >= 5 ? args[4] : null;

            Type inputTaskType;
            Type aggregateTaskType;
            switch( task )
            {
            case "linecount":
                inputTaskType = typeof(LineCounterTask);
                aggregateTaskType = typeof(LineCounterAggregateTask);
                break;
            case "wordcount":
                inputTaskType = typeof(WordCountTask);
                aggregateTaskType = typeof(WordCountAggregateTask);
                break;
            case "readtest":
                ReadTest(input);
                return;
            default:
                Console.WriteLine("Unknown task.");
                return;
            }
            
            Console.WriteLine("Running task {0}, input file {1}, {2} aggregate tasks, output path {3}.", task, input, aggregateTaskCount, output);
            DfsClient dfsClient = new DfsClient();
            IJobServerClientProtocol jobServer = JetClient.CreateJobServerClient();
            Console.WriteLine("Press any key to start");
            Console.ReadKey();
            //Stopwatch sw = new Stopwatch();
            //sw.Start();

            RunJob(dfsClient, jobServer, inputTaskType, aggregateTaskType, input, output, aggregateTaskCount, profileOptions);

            //sw.Stop();
            //Console.WriteLine(sw.Elapsed);

            Console.WriteLine("Done, press any key to exit");

            Console.ReadKey();
        }

        private static void RunJob(DfsClient dfsClient, IJobServerClientProtocol jobServer, Type inputTaskType, Type aggregateTaskType, string fileName, string outputPath, int aggregateTaskCount, string profileOptions)
        {
            const int interval = 5000;
            Guid jobId = StartJob(dfsClient, jobServer, inputTaskType, aggregateTaskType, fileName, outputPath, aggregateTaskCount, profileOptions);
            if( jobId != Guid.Empty )
            {
                JobStatus status;
                while( !jobServer.WaitForJobCompletion(jobId, interval) )
                {
                    status = jobServer.GetJobStatus(jobId);
                    Console.WriteLine(status);
                }
                status = jobServer.GetJobStatus(jobId);
                Console.WriteLine(status);
                Console.WriteLine();
                Console.WriteLine("Job completed.");
                Console.WriteLine("Start time: {0:yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fff}", status.StartTime.ToLocalTime());
                Console.WriteLine("End time:   {0:yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fff}", status.EndTime.ToLocalTime());
                TimeSpan duration = status.EndTime - status.StartTime;
                Console.WriteLine("Duration:   {0} ({1}s)", duration, duration.TotalSeconds);
            }
        }

        private static Guid StartJob(DfsClient dfsClient, IJobServerClientProtocol jobServer, Type inputTaskType, Type aggregateTaskType, string fileName, string outputPath, int aggregateTaskCount, string profileOptions)
        {
            Tkl.Jumbo.Dfs.File file = dfsClient.NameServer.GetFileInfo(fileName);
            if( file == null )
            {
                Console.WriteLine("Input file not found.");
                return Guid.Empty;
            }

            JobConfiguration config = new JobConfiguration()
            {
                AssemblyFileName = Path.GetFileName(inputTaskType.Assembly.Location),
                Tasks = new List<TaskConfiguration>(),
                Channels = new List<ChannelConfiguration>()
            };

            string[] tasks = new string[file.Blocks.Count];
            for( int x = 0; x < file.Blocks.Count; ++x )
            {
                config.Tasks.Add(new TaskConfiguration()
                {
                    TaskID = inputTaskType.Name + (x + 1).ToString(),
                    ProfileOptions = profileOptions,
                    TypeName = inputTaskType.FullName,
                    DfsInput = new TaskDfsInput()
                    {
                        Path = fileName,
                        Block = x,
                        RecordReaderType = typeof(LineRecordReader).AssemblyQualifiedName
                    }
                });
                tasks[x] = inputTaskType.Name + (x + 1).ToString();
            }

            Type interfaceType = FindGenericInterfaceType(aggregateTaskType, typeof(ITask<,>));
            Type outputType = interfaceType.GetGenericArguments()[1];
            Type recordWriterType = typeof(TextRecordWriter<>).MakeGenericType(outputType);
            string[] outputTasks = new string[aggregateTaskCount];
            for( int x = 0; x < aggregateTaskCount; ++x )
            {
                config.Tasks.Add(new TaskConfiguration()
                {
                    TaskID = aggregateTaskType.Name + (x + 1).ToString(),
                    ProfileOptions = profileOptions,
                    TypeName = aggregateTaskType.FullName,
                    DfsOutput = new TaskDfsOutput()
                    {
                        Path = DfsPath.Combine(outputPath, string.Format("result{0}.txt", x + 1)),
                        RecordWriterType = recordWriterType.AssemblyQualifiedName
                    }
                });
                outputTasks[x] = aggregateTaskType.Name + (x + 1).ToString();
            }

            config.Channels.Add(new ChannelConfiguration()
            {
                ChannelType = ChannelType.File,
                InputTasks = tasks,
                OutputTasks = outputTasks,
                PartitionerType = typeof(HashPartitioner<>).MakeGenericType(outputType).AssemblyQualifiedName
            });

            dfsClient.NameServer.Delete(outputPath, true);
            dfsClient.NameServer.CreateDirectory(outputPath);
            Job job = jobServer.CreateJob();
            Console.WriteLine(job.JobID);
            System.IO.File.WriteAllText("jobid.txt", job.JobID.ToString());
            using( DfsOutputStream stream = dfsClient.CreateFile(job.JobConfigurationFilePath) )
            {
                config.SaveXml(stream);
            }
            dfsClient.UploadFile(inputTaskType.Assembly.Location, DfsPath.Combine(job.Path, config.AssemblyFileName));

            jobServer.RunJob(job.JobID);

            return job.JobID;
        }

        private static Type FindGenericInterfaceType(Type type, Type interfaceType)
        {
            // This is necessary because while in .Net you can use type.GetInterface with a generic interface type,
            // in Mono that only works if you specify the type arguments which is precisely what we don't want.
            Type[] interfaces = type.GetInterfaces();
            foreach( Type i in interfaces )
            {
                if( i.IsGenericType && i.GetGenericTypeDefinition() == interfaceType )
                    return i;
            }
            throw new ArgumentException(string.Format("Type {0} does not implement interface {1}.", type, interfaceType));
        }

        private static void ReadTest(string path)
        {
            Console.WriteLine("Reading file {0}.", path);
            DfsClient client = new DfsClient();
            Stopwatch sw = new Stopwatch();
            sw.Start();
            using( DfsInputStream stream = client.OpenFile(path) )
            {
                const int size = 0x10000;
                byte[] buffer = new byte[size];
                while( stream.Read(buffer, 0, size) > 0 )
                {
                }
            }
            sw.Stop();
            Console.WriteLine("Reading file complete: {0}.", sw.ElapsedMilliseconds / 1000.0f);
        }
    }
}
