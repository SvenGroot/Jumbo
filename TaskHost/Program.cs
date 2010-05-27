// $Id$
//
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

            using( ProcessorStatus processorStatus = new ProcessorStatus() )
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();
                string logFile = Path.Combine(jobDirectory, taskId + "_" + attempt.ToString() + ".log");
                ConfigureLog(logFile);

                _log.InfoFormat("Running task; job ID = \"{0}\", job directory = \"{1}\", task ID = \"{2}\", attempt, = {3}, DFS job directory = \"{4}\"", jobId, jobDirectory, taskId, attempt, dfsJobDirectory);
                _log.DebugFormat("Command line: {0}", Environment.CommandLine);
                _log.LogEnvironmentInformation();

                _log.Info("Loading configuration.");
                string configDirectory = Path.Combine(jobDirectory, "config");
                DfsConfiguration dfsConfig = DfsConfiguration.FromXml(Path.Combine(configDirectory, "dfs.config"));
                JetConfiguration jetConfig = JetConfiguration.FromXml(Path.Combine(configDirectory, "jet.config"));

                _log.Info("Creating RPC clients.");
                ITaskServerUmbilicalProtocol umbilical = JetClient.CreateTaskServerUmbilicalClient(jetConfig.TaskServer.Port);
                _dfsClient = new DfsClient(dfsConfig);
                _jetClient = new JetClient(jetConfig);


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
                    using( TaskExecutionUtility taskExecution = TaskExecutionUtility.Create(_dfsClient, _jetClient, umbilical, jobId, config, new TaskId(taskId), dfsJobDirectory, jobDirectory, attempt) )
                    {
                        taskExecution.RunTask();
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
                processorStatus.Refresh();
                _log.InfoFormat("Processor usage during this task (system-wide, not process specific):");
                _log.Info(processorStatus.Total);
            }
            
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

         private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            _log.Fatal("An unhandled exception occurred.", (Exception)e.ExceptionObject);
        } 
    }
}
