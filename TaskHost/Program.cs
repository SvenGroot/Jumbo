using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using System.IO;
using System.Reflection;
using Tkl.Jumbo.Dfs;

namespace TaskHost
{
    class Program
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(Program));

        public static int Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);

            if( args.Length != 3 )
            {
                _log.Error("Invalid invocation.");
                return 1;
            }

            string jobDirectory = args[0];
            string taskID = args[1];
            string logFile = args[2];
            ConfigureLog(logFile);

            _log.InfoFormat("Running task; job directory = \"{0}\", task ID = \"{1}\"", jobDirectory, taskID);

            JobConfiguration config = JobConfiguration.LoadXml(Path.Combine(jobDirectory, Job.JobConfigFileName));

            TaskConfiguration taskConfig = config.GetTask(taskID);
            if( taskConfig == null )
            {
                _log.ErrorFormat("Task {0} does not exist in the job configuration.", taskID);
                return 1;
            }

            RunTask(jobDirectory, config, taskConfig);

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

        private static void RunTask(string jobDirectory, JobConfiguration jobConfig, TaskConfiguration taskConfig)
        {
            Assembly taskAssembly = Assembly.LoadFrom(Path.Combine(jobDirectory, jobConfig.AssemblyFileName));

            Type taskType = taskAssembly.GetType(taskConfig.TypeName);
            Type taskInterfaceType = taskType.GetInterface(typeof(ITask<object>).GetGenericTypeDefinition().FullName);
            Type inputType = taskInterfaceType.GetGenericArguments()[0];

            MethodInfo doRunTaskMethod = typeof(Program).GetMethod("DoRunTask", BindingFlags.NonPublic | BindingFlags.Static).MakeGenericMethod(inputType);
            doRunTaskMethod.Invoke(null, new object[] { taskType, taskConfig });
            //ITask task = (ITask)taskAssembly.CreateInstance(taskConfig.TypeName);
            //task.Run();
        }

        private static void DoRunTask<T>(Type taskType, TaskConfiguration taskConfig)
        {
            using( DfsInputStream inputStream = OpenInputFile(taskConfig) )
            using( RecordReader<T> input = CreateRecordReader<T>(inputStream, taskConfig) )
            {
                ITask<T> task = (ITask<T>)Activator.CreateInstance(taskType);
                task.Run(input);
            }
        }

        private static DfsInputStream OpenInputFile(TaskConfiguration taskConfig)
        {
            if( taskConfig.DfsInput != null )
            {
                DfsClient client = new DfsClient();
                return client.OpenFile(taskConfig.DfsInput.Path);
            }
            return null;
        }

        private static RecordReader<T> CreateRecordReader<T>(Stream inputStream, TaskConfiguration taskConfig)
        {
            if( taskConfig.DfsInput != null )
            {
                Type recordReaderType = Type.GetType(taskConfig.DfsInput.RecordReaderType);
                return (RecordReader<T>)Activator.CreateInstance(recordReaderType, inputStream, taskConfig.DfsInput.Offset, taskConfig.DfsInput.Size);
            }
            return null;
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            _log.Fatal("An unhandled exception occurred.", (Exception)e.ExceptionObject);
        }
    }
}
