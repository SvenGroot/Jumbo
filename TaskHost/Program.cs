﻿using System;
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
	  // It's the constructor that's important for AssemblyResolver,
	  // so disable the warning about the field not being used.
#pragma warning disable 414
        private static readonly AssemblyResolver _resolver = new AssemblyResolver();
#pragma warning restore 414
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(Program));
        private static DfsClient _dfsClient;
        private static JetClient _jetClient;

        public static int Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);

            if( args.Length != 5 )
            {
                _log.Error("Invalid invocation.");
                return 1;
            }

            Guid jobId = new Guid(args[0]);
            string jobDirectory = args[1];
            string taskId = args[2];
            string dfsJobDirectory = args[3];
            int attempt = Convert.ToInt32(args[4]);

            string configDirectory = Path.Combine(jobDirectory, "config");
            DfsConfiguration dfsConfig = DfsConfiguration.FromXml(Path.Combine(configDirectory, "dfs.config"));
            JetConfiguration jetConfig = JetConfiguration.FromXml(Path.Combine(configDirectory, "jet.config"));

			_log.DebugFormat("Merge task buffer size: {0}", jetConfig.FileChannel.MergeTaskReadBufferSize);
            ITaskServerUmbilicalProtocol umbilical = JetClient.CreateTaskServerUmbilicalClient(jetConfig.TaskServer.Port);
            _dfsClient = new DfsClient(dfsConfig);
            _jetClient = new JetClient(jetConfig);

            Stopwatch sw = new Stopwatch();
            sw.Start();
            string logFile = Path.Combine(jobDirectory, taskId + "_" + attempt.ToString() + ".log");
            ConfigureLog(logFile);

            _log.InfoFormat("Running task; job ID = \"{0}\", job directory = \"{1}\", task ID = \"{2}\", attempt, = {3}, DFS job directory = \"{4}\"", jobId, jobDirectory, taskId, attempt, dfsJobDirectory);
            _log.DebugFormat("Command line: {0}", Environment.CommandLine);
            _log.LogEnvironmentInformation();

            string xmlConfigPath = Path.Combine(jobDirectory, Job.JobConfigFileName);
            _log.DebugFormat("Loading job configuration from local file {0}.", xmlConfigPath);
            JobConfiguration config = JobConfiguration.LoadXml(xmlConfigPath);
            _log.Debug("Job configuration loaded.");

            if( config.AssemblyFileNames != null )
            {
                foreach( string assemblyFileName in config.AssemblyFileNames )
                {
                    _log.DebugFormat("Loading assembly {0}.", assemblyFileName);
                    Assembly.LoadFrom(Path.Combine(jobDirectory, assemblyFileName));
                }
            }
            
            try
            {
                using( TaskExecutionUtility taskExecution = new TaskExecutionUtility(_jetClient, umbilical, jobId, config, taskId, _dfsClient, jobDirectory, dfsJobDirectory, attempt) )
                {
                    RunTask(taskExecution);
                }

                sw.Stop();

                _log.Info("Reporting completion to task server.");
                umbilical.ReportCompletion(jobId, taskId);
            }
            catch( Exception ex )
            {
                _log.Fatal("Failed to execute task.", ex);
            }
            _log.InfoFormat("Task host finished execution of task, execution time: {0}s", sw.Elapsed.TotalSeconds);
            return 0;
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
            // Lifetime is managed by the TaskExecutionUtility class, no need to put them in a using block.
            RecordWriter<TOutput> output = taskExecution.GetOutputWriter<TOutput>();
            Stopwatch taskStopwatch = new Stopwatch();

            IMergeTask<TInput, TOutput> mergeTask = task as IMergeTask<TInput, TOutput>;
            if( mergeTask != null )
            {
                // Lifetime is managed by the TaskExecutionUtility class, no need to put them in a using block.
                MergeTaskInput<TInput> input = taskExecution.GetMergeTaskInput<TInput>();

                _log.Info("Running merge task.");
                taskStopwatch.Start();
                mergeTask.Run(input, output);
                taskStopwatch.Stop();
                _log.InfoFormat("Task finished execution, execution time: {0}s", taskStopwatch.Elapsed.TotalSeconds);
            }
            else
            {
                IPullTask<TInput, TOutput> pullTask = task as IPullTask<TInput, TOutput>;

                // Lifetime is managed by the TaskExecutionUtility class, no need to put them in a using block.
                RecordReader<TInput> input = taskExecution.GetInputReader<TInput>();

                if( pullTask != null )
                {
                    _log.Info("Running pull task.");
                    taskStopwatch.Start();
                    pullTask.Run(input, output);
                    taskStopwatch.Stop();
                }
                else
                {
                    _log.Info("Running push task.");
                    IPushTask<TInput, TOutput> pushTask = (IPushTask<TInput, TOutput>)task;
                    taskStopwatch.Start();
                    foreach( TInput record in input.EnumerateRecords() )
                    {
                        pushTask.ProcessRecord(record, output);
                    }
                    // Finish is called by taskExecution.FinishTask below.
                    taskStopwatch.Stop();
                }
                TimeSpan timeWaiting;
                MultiRecordReader<TInput> multiReader = input as MultiRecordReader<TInput>;
                if( multiReader != null )
                    timeWaiting = multiReader.TimeWaiting;
                else
                    timeWaiting = TimeSpan.Zero;
                _log.InfoFormat("Task finished execution, execution time: {0}s; time spent waiting for input: {1}s.", taskStopwatch.Elapsed.TotalSeconds, timeWaiting.TotalSeconds);
            }

            taskExecution.FinishTask();


            // TODO: Proper metrics for pipelined tasks.
            TaskMetrics metrics = taskExecution.CalculateMetrics();
            _log.Info(metrics);
        }

#pragma warning restore 0169

         private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            _log.Fatal("An unhandled exception occurred.", (Exception)e.ExceptionObject);
        } 
    }
}
