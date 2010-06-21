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
    public static class Program
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(Program));

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
            TaskAttemptId taskAttemptId = new TaskAttemptId(new TaskId(taskId), attempt);

            TaskExecutionUtility.RunTask(jobId, jobDirectory, dfsJobDirectory, taskAttemptId);
            
            return 0;
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            _log.Fatal("An unhandled exception occurred.", (Exception)e.ExceptionObject);
        } 
    }
}
