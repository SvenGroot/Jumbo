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
        private static int _attempt;

        public static int Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);

            if( args.Length != 10 )
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
            _attempt = Convert.ToInt32(args[9]);
            string logFile = Path.Combine(jobDirectory, taskID + "_" + _attempt.ToString() + ".log");
            ConfigureLog(logFile);

            _log.InfoFormat("Running task; job ID = \"{0}\", job directory = \"{1}\", task ID = \"{2}\", attempt, = {3}, DFS job directory = \"{4}\"", jobID, jobDirectory, taskID, _attempt, dfsJobDirectory);
            _log.LogEnvironmentInformation();
            _log.DebugFormat("Command line: {0}", Environment.CommandLine);

            _log.Debug("Creating name server, job server and umbilical clients.");
            ITaskServerUmbilicalProtocol umbilical = JetClient.CreateTaskServerUmbilicalClient(umbilicalPort);
            _client = new DfsClient(nameServerHost, nameServerPort);
            _jobServer = JetClient.CreateJobServerClient(jobServerHost, jobServerPort);

            string xmlConfigPath = Path.Combine(jobDirectory, Job.JobConfigFileName);
            _log.DebugFormat("Loading job configuration from local file {0}.", xmlConfigPath);
            JobConfiguration config = JobConfiguration.LoadXml(xmlConfigPath);
            _log.Debug("Job configuration loaded.");

            TaskConfiguration taskConfig = config.GetTask(taskID);
            if( taskConfig == null )
            {
                _log.ErrorFormat("Task {0} does not exist in the job configuration.", taskID);
                return 1;
            }

            RunTask(jobID, jobDirectory, config, taskConfig, dfsJobDirectory);

            _log.Info("Reporting completion to task server.");
            umbilical.ReportCompletion(jobID, taskID);

            _log.Info("TaskHost finished execution.");
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
            string assemblyPath = Path.Combine(jobDirectory, jobConfig.AssemblyFileName);
            _log.DebugFormat("Loading task assembly from {0}", assemblyPath);
            Assembly taskAssembly = Assembly.LoadFrom(assemblyPath);

            _log.DebugFormat("Determining task type and input and output types.");
            Type taskType = taskAssembly.GetType(taskConfig.TypeName);
            Type taskInterfaceType = FindGenericInterfaceType(taskType, typeof(ITask<,>));
            Type inputType = taskInterfaceType.GetGenericArguments()[0];
            Type outputType = taskInterfaceType.GetGenericArguments()[1];
            _log.InfoFormat("Input type: {0}", inputType.AssemblyQualifiedName);
            _log.InfoFormat("Output type: {0}", outputType.AssemblyQualifiedName);

            ChannelConfiguration inputChannelConfig = jobConfig.GetInputChannelForTask(taskConfig.TaskID);
            IInputChannel inputChannel = null;
            if( inputChannelConfig != null )
            {
                _log.DebugFormat("Creating input channel {0}.", inputChannelConfig.ChannelType);
                inputChannel = inputChannelConfig.CreateInputChannel(jobID, jobDirectory, _jobServer, taskConfig.TaskID);
                inputChannel.WaitUntilReady(Timeout.Infinite);
            }

            ChannelConfiguration outputChannelConfig = jobConfig.GetOutputChannelForTask(taskConfig.TaskID);
            IOutputChannel outputChannel = null;
            if( outputChannelConfig != null )
            {
                _log.DebugFormat("Creating output channel {0}, partitioner {1}.", outputChannelConfig.ChannelType, outputChannelConfig.PartitionerType);
                outputChannel = outputChannelConfig.CreateOutputChannel(jobDirectory, taskConfig.TaskID);
            }

            _log.Debug("Creating generic method to run task.");
            MethodInfo doRunTaskMethod = typeof(Program)
                                            .GetMethod("DoRunTask", BindingFlags.NonPublic | BindingFlags.Static)
                                            .MakeGenericMethod(inputType, outputType);
            _log.Debug("Invoking generic method.");
            doRunTaskMethod.Invoke(null, new object[] { taskType, taskConfig, inputChannel, outputChannel, dfsJobDirectory });
        }

        private static void DoRunTask<TInput, TOutput>(Type taskType, TaskConfiguration taskConfig, IInputChannel inputChannel, IOutputChannel outputChannel, string dfsJobDirectory) 
            where TInput : IWritable, new()
            where TOutput : IWritable, new()
        {
            _log.Debug("DoRunTask invoked.");
            using( DfsInputStream inputStream = OpenInputFile(taskConfig) )
            using( RecordReader<TInput> input = CreateRecordReader<TInput>(inputStream, taskConfig, inputChannel) )
            using( DfsOutputStream outputStream = OpenOutputFile(taskConfig, dfsJobDirectory) )
            using( RecordWriter<TOutput> output = CreateRecordWriter<TOutput>(outputStream, taskConfig, outputChannel) )
            {
                _log.DebugFormat("Creating {0} task instance.", taskType.AssemblyQualifiedName);
                ITask<TInput, TOutput> task = (ITask<TInput, TOutput>)Activator.CreateInstance(taskType);
                _log.Info("Running task.");
                task.Run(input, output);
                _log.Info("Task finished execution.");
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
                _log.DebugFormat("Opening input file {0}", taskConfig.DfsInput.Path);
                return _client.OpenFile(taskConfig.DfsInput.Path);
            }
            return null;
        }

        private static RecordReader<T> CreateRecordReader<T>(Stream inputStream, TaskConfiguration taskConfig, IInputChannel inputChannel) 
            where T : IWritable, new()
        {
            if( taskConfig.DfsInput != null )
            {
                _log.DebugFormat("Creating record reader of type {0}", taskConfig.DfsInput.RecordReaderType);
                Type recordReaderType = Type.GetType(taskConfig.DfsInput.RecordReaderType);
                long offset;
                long size;
                long blockSize = _client.NameServer.BlockSize;
                offset = blockSize * (long)taskConfig.DfsInput.Block;
                size = Math.Min(blockSize, _client.NameServer.GetFileInfo(taskConfig.DfsInput.Path).Size - offset);
                return (RecordReader<T>)Activator.CreateInstance(recordReaderType, inputStream, offset, size);
            }
            else if( inputChannel != null )
            {
                _log.Debug("Creating input channel record reader.");
                return inputChannel.CreateRecordReader<T>();
            }
            return null;
        }

        private static DfsOutputStream OpenOutputFile(TaskConfiguration taskConfig, string dfsJobDirectory)
        {
            if( taskConfig.DfsOutput != null )
            {
                string file = DfsPath.Combine(dfsJobDirectory, taskConfig.TaskID + "_" + _attempt.ToString());
                _log.DebugFormat("Opening output file {0}", file);
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
                _log.DebugFormat("Creating record writer of type {0}", taskConfig.DfsOutput.RecordWriterType);
                Type recordWriterType = Type.GetType(taskConfig.DfsOutput.RecordWriterType);
                return (StreamRecordWriter<T>)Activator.CreateInstance(recordWriterType, outputStream);
            }
            else if( outputChannel != null )
            {
                _log.DebugFormat("Creating output channel record writer.");
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
