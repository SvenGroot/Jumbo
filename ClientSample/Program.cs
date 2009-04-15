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
            log4net.Config.BasicConfigurator.Configure();
            log4net.LogManager.GetRepository().Threshold = log4net.Core.Level.Info;

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

            DfsClient dfsClient = new DfsClient();
            JetClient jetClient = new JetClient();
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
            case "graysort":
                Console.WriteLine("Running job GraySort, input file {0}, {1} aggregate tasks, output path {2}.", input, aggregateTaskCount, output);
                Console.WriteLine("Press any key to start");
                Console.ReadKey();
                Guid id = GraySort.GraySortJob.RunJob(jetClient, dfsClient, input, output, aggregateTaskCount);
                WaitForJobCompletion(jetClient, 5000, id);
                Console.WriteLine("Done, press any key to exit");
                Console.ReadKey();
                return;
            default:
                Console.WriteLine("Unknown task.");
                return;
            }
            
            Console.WriteLine("Running task {0}, input file {1}, {2} aggregate tasks, output path {3}.", task, input, aggregateTaskCount, output);
            Console.WriteLine("Press any key to start");
            Console.ReadKey();
            //Stopwatch sw = new Stopwatch();
            //sw.Start();

            RunJob(dfsClient, jetClient, inputTaskType, aggregateTaskType, input, output, aggregateTaskCount, profileOptions);

            //sw.Stop();
            //Console.WriteLine(sw.Elapsed);

            Console.WriteLine("Done, press any key to exit");

            Console.ReadKey();
        }

        private static void RunJob(DfsClient dfsClient, JetClient jetClient, Type inputTaskType, Type aggregateTaskType, string fileName, string outputPath, int aggregateTaskCount, string profileOptions)
        {
            const int interval = 5000;
            Guid jobId = StartJob(dfsClient, jetClient, inputTaskType, aggregateTaskType, fileName, outputPath, aggregateTaskCount, profileOptions);
            if( jobId != Guid.Empty )
            {
                jobId = WaitForJobCompletion(jetClient, interval, jobId);
            }
        }

        private static Guid WaitForJobCompletion(JetClient jetClient, int interval, Guid jobId)
        {
            JobStatus status;
            while( !jetClient.JobServer.WaitForJobCompletion(jobId, interval) )
            {
                status = jetClient.JobServer.GetJobStatus(jobId);
                Console.WriteLine(status);
            }
            status = jetClient.JobServer.GetJobStatus(jobId);
            Console.WriteLine(status);
            Console.WriteLine();
            Console.WriteLine("Job completed.");
            Console.WriteLine("Start time: {0:yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fff}", status.StartTime.ToLocalTime());
            Console.WriteLine("End time:   {0:yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fff}", status.EndTime.ToLocalTime());
            TimeSpan duration = status.EndTime - status.StartTime;
            Console.WriteLine("Duration:   {0} ({1}s)", duration, duration.TotalSeconds);
            return jobId;
        }

        private static Guid StartJob(DfsClient dfsClient, JetClient jetClient, Type inputTaskType, Type aggregateTaskType, string fileName, string outputPath, int aggregateTaskCount, string profileOptions)
        {
            Tkl.Jumbo.Dfs.File file = dfsClient.NameServer.GetFileInfo(fileName);
            if( file == null )
            {
                Console.WriteLine("Input file not found.");
                return Guid.Empty;
            }

            dfsClient.NameServer.Delete(outputPath, true);
            dfsClient.NameServer.CreateDirectory(outputPath);

            JobConfiguration config = new JobConfiguration(inputTaskType.Assembly);
            config.AddInputStage(inputTaskType.Name, file, inputTaskType, typeof(LineRecordReader));
            Type interfaceType = FindGenericInterfaceType(aggregateTaskType, typeof(ITask<,>));
            Type outputType = interfaceType.GetGenericArguments()[1];
            config.AddStage(aggregateTaskType.Name, new[] { inputTaskType.Name }, aggregateTaskType, aggregateTaskCount, ChannelType.File, null, outputPath, typeof(TextRecordWriter<>).MakeGenericType(outputType));


            Job job = jetClient.RunJob(config, dfsClient, inputTaskType.Assembly.Location);

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
