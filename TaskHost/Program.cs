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
using System.Diagnostics;

namespace TaskHost
{
    static class Program
    {
        private static readonly AssemblyResolver _resolver = new AssemblyResolver();
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(Program));
        private static DfsClient _dfsClient;
        private static JetClient _jetClient;
        private static int _blockSize;

        public static int Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);

            if( args.Length != 6 )
            {
                _log.Error("Invalid invocation.");
                return 1;
            }

            int instanceId = Convert.ToInt32(args[0]);
            int umbilicalPort = Convert.ToInt32(args[1]);
            string jobServerHost = args[2];
            int jobServerPort = Convert.ToInt32(args[3]);
            string nameServerHost = args[4];
            int nameServerPort = Convert.ToInt32(args[5]);

            ITaskServerUmbilicalProtocol umbilical = JetClient.CreateTaskServerUmbilicalClient(umbilicalPort);
            _dfsClient = new DfsClient(nameServerHost, nameServerPort);
            _jetClient = new JetClient(jobServerHost, jobServerPort);
            _blockSize = _dfsClient.NameServer.BlockSize;

            while( true )
            {
                TaskExecutionInfo taskInfo = null;
                try
                {
                    taskInfo = umbilical.WaitForTask(instanceId, 10000);
                }
                catch( ServerShutdownException )
                {
                    return 0;
                }
                if( taskInfo != null )
                {
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    string logFile = Path.Combine(taskInfo.JobDirectory, taskInfo.TaskId + "_" + taskInfo.Attempt.ToString() + ".log");
                    ConfigureLog(logFile);

                    _log.InfoFormat("Running task; job ID = \"{0}\", job directory = \"{1}\", task ID = \"{2}\", attempt, = {3}, DFS job directory = \"{4}\"", taskInfo.JobId, taskInfo.JobDirectory, taskInfo.TaskId, taskInfo.Attempt, taskInfo.DfsJobDirectory);
                    _log.LogEnvironmentInformation();

                    string xmlConfigPath = Path.Combine(taskInfo.JobDirectory, Job.JobConfigFileName);
                    _log.DebugFormat("Loading job configuration from local file {0}.", xmlConfigPath);
                    JobConfiguration config = JobConfiguration.LoadXml(xmlConfigPath);
                    _log.Debug("Job configuration loaded.");

                    if( config.AssemblyFileNames != null )
                    {
                        foreach( string assemblyFileName in config.AssemblyFileNames )
                        {
                            _log.DebugFormat("Loading assembly {0}.", assemblyFileName);
                            Assembly.LoadFrom(Path.Combine(taskInfo.JobDirectory, assemblyFileName));
                        }
                    }

                    using( TaskExecutionUtility taskExecution = new TaskExecutionUtility(_jetClient, taskInfo.JobId, config, taskInfo.TaskId, _dfsClient, taskInfo.JobDirectory, taskInfo.DfsJobDirectory, taskInfo.Attempt) )
                    {
                        RunTask(taskExecution);
                    }

                    sw.Stop();

                    _log.Info("Reporting completion to task server.");
                    umbilical.ReportCompletion(taskInfo.JobId, taskInfo.TaskId);

                    _log.InfoFormat("Task host finished execution of task, execution time: {0}s", sw.Elapsed.TotalSeconds);
                }
            }
        }

        private static void ConfigureLog(string logFile)
        {
            log4net.LogManager.ResetConfiguration();
            log4net.Appender.FileAppender appender = new log4net.Appender.FileAppender()
            {
                File = logFile,
                Layout = new log4net.Layout.PatternLayout("%date [%thread] %-5level %logger - %message%newline"),
                Threshold = log4net.Core.Level.All
            };
            appender.ActivateOptions();
            log4net.Config.BasicConfigurator.Configure(appender);
            
        }

        private static void RunTask(TaskExecutionUtility taskExecution)
        {
            _log.Debug("Creating generic method to run task.");
            MethodInfo doRunTaskMethod = typeof(Program)
                                            .GetMethod("DoRunTask", BindingFlags.NonPublic | BindingFlags.Static)
                                            .MakeGenericMethod(taskExecution.InputRecordType, taskExecution.OutputRecordType);
            _log.Debug("Invoking generic method.");
            doRunTaskMethod.Invoke(null, new object[] { taskExecution });
        }

#pragma warning disable 0169 // Disable private member not used warning in Mono C# compiler; it's used with reflection.

        private static void DoRunTask<TInput, TOutput>(TaskExecutionUtility taskExecution) 
            where TInput : IWritable, new()
            where TOutput : IWritable, new()
        {
            _log.Debug("DoRunTask invoked.");
            ITask<TInput, TOutput> task = taskExecution.GetTaskInstance<TInput, TOutput>();
            _log.Info("Running task.");
            IPullTask<TInput, TOutput> pullTask = task as IPullTask<TInput, TOutput>;
            Stopwatch taskStopwatch = new Stopwatch();

            // Lifetime is managed by the TaskExecutionUtility class, no need to put them in a using block.
            RecordReader<TInput> input = taskExecution.GetInputReader<TInput>();
            RecordWriter<TOutput> output = taskExecution.GetOutputWriter<TOutput>();

            if( pullTask != null )
            {
                taskStopwatch.Start();
                pullTask.Run(input, output);
                taskStopwatch.Stop();
            }
            else
            {
                IPushTask<TInput, TOutput> pushTask = (IPushTask<TInput, TOutput>)task;
                taskStopwatch.Start();
                foreach( TInput record in input.EnumerateRecords() )
                {
                    pushTask.ProcessRecord(record, output);
                }
                // Finish is called by taskExecution.FinishTask below.
                taskStopwatch.Stop();
            }

            taskExecution.FinishTask();

            TimeSpan timeWaiting;
            MultiRecordReader<TInput> multiReader = input as MultiRecordReader<TInput>;
            if( multiReader != null )
                timeWaiting = multiReader.TimeWaiting;
            else
                timeWaiting = TimeSpan.Zero;
            _log.InfoFormat("Task finished execution, execution time: {0}s; time spent waiting for input: {1}s.", taskStopwatch.Elapsed.TotalSeconds, timeWaiting.TotalSeconds);

            // TODO: Proper metrics for pipelined tasks.
            //TaskMetrics metrics = CalculateMetrics<TInput, TOutput>(taskExecution.TaskConfiguration, taskExecution.InputChannel, taskExecution.OutputChannel, inputStream, input, dfsOutputs, output);
            //_log.Info(metrics);
        }

#pragma warning restore 0169

         private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            _log.Fatal("An unhandled exception occurred.", (Exception)e.ExceptionObject);
        }

        //private static TaskMetrics CalculateMetrics<TInput, TOutput>(TaskConfiguration taskConfig, IInputChannel inputChannel, IOutputChannel outputChannel, DfsInputStream inputStream, RecordReader<TInput> input, List<TaskDfsOutput> outputs, RecordWriter<TOutput> output)
        //    where TInput : IWritable, new()
        //    where TOutput : IWritable, new()
        //{
        //    TaskMetrics metrics = new TaskMetrics();
        //    if( input != null )
        //        metrics.RecordsRead = input.RecordsRead;
        //    if( output != null )
        //        metrics.RecordsWritten += output.RecordsWritten;

        //    if( taskConfig.DfsInput != null )
        //        metrics.DfsBytesRead = inputStream.Position - (taskConfig.DfsInput.Block * (long)_blockSize);
        //    else
        //    {
        //        FileInputChannel fileInputChannel = inputChannel as FileInputChannel;
        //        if( fileInputChannel != null )
        //        {
        //            metrics.LocalBytesRead = fileInputChannel.LocalBytesRead;
        //            metrics.NetworkBytesRead = fileInputChannel.NetworkBytesRead;
        //        }
        //    }

        //    if( taskConfig.DfsOutput != null )
        //        metrics.DfsBytesWritten = (from o in outputs select o.Stream.Length).Sum();
        //    else if( outputChannel is FileOutputChannel )
        //    {
        //        StreamRecordWriter<TOutput> streamOutput = output as StreamRecordWriter<TOutput>;
        //        if( streamOutput != null )
        //        {
        //            metrics.LocalBytesWritten = streamOutput.Stream.Length;
        //        }
        //        else
        //        {
        //            MultiRecordWriter<TOutput> multiOutput = output as MultiRecordWriter<TOutput>;
        //            if( multiOutput != null )
        //            {
        //                metrics.LocalBytesWritten = (from writer in multiOutput.Writers
        //                                             let streamWriter = writer as StreamRecordWriter<TOutput>
        //                                             where streamWriter != null
        //                                             select streamWriter.Stream.Length).Sum();
        //            }
        //        }
        //    }
        //    return metrics;
        //}    
    }
}
