using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using System.IO;
using System.Reflection;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.IO;
using System.Threading;
using Tkl.Jumbo;

namespace TaskHost
{
    class Program
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(Program));
        private static DfsClient _client;
        private static IJobServerClientProtocol _jobServer;

        public static int Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);

            if( args.Length != 9 )
            {
                _log.Error("Invalid invocation.");
                return 1;
            }

            Guid jobID = new Guid(args[0]);
            string jobDirectory = args[1];
            string taskID = args[2];
            string dfsJobDirectory = args[3];
            int umbilicalPort = Convert.ToInt32(args[4]);
            string jobServerHost = args[5];
            int jobServerPort = Convert.ToInt32(args[6]);
            string nameServerHost = args[7];
            int nameServerPort = Convert.ToInt32(args[8]);
            string logFile = Path.Combine(jobDirectory, taskID + ".log");
            ConfigureLog(logFile);

            ITaskServerUmbilicalProtocol umbilical = JetClient.CreateTaskServerUmbilicalClient(umbilicalPort);
            _client = new DfsClient(nameServerHost, nameServerPort);
            _jobServer = JetClient.CreateJobServerClient(jobServerHost, jobServerPort);

            _log.InfoFormat("Running task; job ID = \"{0}\", job directory = \"{1}\", task ID = \"{2}\", DFS job directory = \"{3}\"", jobID, jobDirectory, taskID, dfsJobDirectory);
            _log.LogEnvironmentInformation();

            JobConfiguration config = JobConfiguration.LoadXml(Path.Combine(jobDirectory, Job.JobConfigFileName));

            TaskConfiguration taskConfig = config.GetTask(taskID);
            if( taskConfig == null )
            {
                _log.ErrorFormat("Task {0} does not exist in the job configuration.", taskID);
                return 1;
            }

            RunTask(jobID, jobDirectory, config, taskConfig, dfsJobDirectory);

            umbilical.ReportCompletion(jobID, taskID);
            _log.Info("Task execution finished.");

            return 0;
        }

        private static void ConfigureLog(string logFile)
        {
            log4net.Appender.FileAppender appender = new log4net.Appender.FileAppender()
            {
                File = logFile,
                Layout = new log4net.Layout.PatternLayout("%date [%thread] %-5level %logger - %message%newline"),
                Threshold = log4net.Core.Level.All
            };
            appender.ActivateOptions();
            log4net.Config.BasicConfigurator.Configure(appender);
            
        }

        private static void RunTask(Guid jobID, string jobDirectory, JobConfiguration jobConfig, TaskConfiguration taskConfig, string dfsJobDirectory)
        {
            Assembly taskAssembly = Assembly.LoadFrom(Path.Combine(jobDirectory, jobConfig.AssemblyFileName));

            Type taskType = taskAssembly.GetType(taskConfig.TypeName);
            Type taskInterfaceType = FindGenericInterfaceType(taskType, (typeof(ITask<StringWritable, StringWritable>).GetGenericTypeDefinition()));
            Type inputType = taskInterfaceType.GetGenericArguments()[0];
            Type outputType = taskInterfaceType.GetGenericArguments()[1];

            ChannelConfiguration inputChannelConfig = jobConfig.GetInputChannelForTask(taskConfig.TaskID);
            IInputChannel inputChannel = null;
            if( inputChannelConfig != null )
            {
                inputChannel = inputChannelConfig.CreateInputChannel(jobID, jobDirectory, _jobServer);
                inputChannel.WaitUntilReady(Timeout.Infinite);
            }

            ChannelConfiguration outputChannelConfig = jobConfig.GetOutputChannelForTask(taskConfig.TaskID);
            IOutputChannel outputChannel = null;
            if( outputChannelConfig != null )
                outputChannel = outputChannelConfig.CreateOutputChannel(jobDirectory, taskConfig.TaskID);

            MethodInfo doRunTaskMethod = typeof(Program)
                                            .GetMethod("DoRunTask", BindingFlags.NonPublic | BindingFlags.Static)
                                            .MakeGenericMethod(inputType, outputType);
            doRunTaskMethod.Invoke(null, new object[] { taskType, taskConfig, inputChannel, outputChannel, dfsJobDirectory });
        }

        private static void DoRunTask<TInput, TOutput>(Type taskType, TaskConfiguration taskConfig, IInputChannel inputChannel, IOutputChannel outputChannel, string dfsJobDirectory) 
            where TInput : IWritable, new()
            where TOutput : IWritable, new()
        {
            using( DfsInputStream inputStream = OpenInputFile(taskConfig) )
            using( RecordReader<TInput> input = CreateRecordReader<TInput>(inputStream, taskConfig, inputChannel) )
            using( DfsOutputStream outputStream = OpenOutputFile(taskConfig, dfsJobDirectory) )
            using( RecordWriter<TOutput> output = CreateRecordWriter<TOutput>(outputStream, taskConfig, outputChannel) )
            {
                ITask<TInput, TOutput> task = (ITask<TInput, TOutput>)Activator.CreateInstance(taskType);
                task.Run(input, output);
            }

            if( taskConfig.DfsOutput != null )
            {
                _log.InfoFormat("Moving task output file from \"{0}\" to \"{1}\".", taskConfig.DfsOutput.TempPath, taskConfig.DfsOutput.Path);
                _client.NameServer.Move(taskConfig.DfsOutput.TempPath, taskConfig.DfsOutput.Path);
            }
        }

        private static DfsInputStream OpenInputFile(TaskConfiguration taskConfig)
        {
            if( taskConfig.DfsInput != null )
            {
                return _client.OpenFile(taskConfig.DfsInput.Path);
            }
            return null;
        }

        private static RecordReader<T> CreateRecordReader<T>(Stream inputStream, TaskConfiguration taskConfig, IInputChannel inputChannel) 
            where T : IWritable, new()
        {
            if( taskConfig.DfsInput != null )
            {
                Type recordReaderType = Type.GetType(taskConfig.DfsInput.RecordReaderType);
                long offset;
                long size;
                int blockSize = _client.NameServer.BlockSize;
                offset = blockSize * taskConfig.DfsInput.Block;
                size = Math.Min(blockSize, _client.NameServer.GetFileInfo(taskConfig.DfsInput.Path).Size - offset);
                return (RecordReader<T>)Activator.CreateInstance(recordReaderType, inputStream, offset, size);
            }
            else if( inputChannel != null )
            {
                return inputChannel.CreateRecordReader<T>();
            }
            return null;
        }

        private static DfsOutputStream OpenOutputFile(TaskConfiguration taskConfig, string dfsJobDirectory)
        {
            if( taskConfig.DfsOutput != null )
            {
                string file = DfsPath.Combine(dfsJobDirectory, taskConfig.TaskID);
                taskConfig.DfsOutput.TempPath = file;
                return _client.CreateFile(file);
            }
            return null;
        }

        private static RecordWriter<T> CreateRecordWriter<T>(Stream outputStream, TaskConfiguration taskConfig, IOutputChannel outputChannel)
            where T : IWritable, new()
        {
            if( taskConfig.DfsOutput != null )
            {
                Type recordWriterType = Type.GetType(taskConfig.DfsOutput.RecordWriterType);
                return (RecordWriter<T>)Activator.CreateInstance(recordWriterType, outputStream);
            }
            else if( outputChannel != null )
            {
                return outputChannel.CreateRecordWriter<T>();
            }
            return null;
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            _log.Fatal("An unhandled exception occurred.", (Exception)e.ExceptionObject);
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
    }
}
