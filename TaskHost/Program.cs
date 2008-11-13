using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using System.IO;
using System.Reflection;

namespace TaskHost
{
    class Program
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(Program));

        public static int Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);

            if( args.Length != 2 )
            {
                _log.Error("Invalid invocation.");
                return 1;
            }

            string jobDirectory = args[0];
            string taskID = args[1];
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

        private static void RunTask(string jobDirectory, JobConfiguration jobConfig, TaskConfiguration taskConfig)
        {
            Assembly taskAssembly = Assembly.LoadFrom(Path.Combine(jobDirectory, jobConfig.AssemblyFileName));

            ITask task = (ITask)taskAssembly.CreateInstance(taskConfig.TypeName);
            task.Run();
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            _log.Fatal("An unhandled exception occurred.", (Exception)e.ExceptionObject);
        }
    }
}
