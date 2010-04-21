using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using System.Reflection;
using Tkl.Jumbo.Jet.Jobs;
using Tkl.Jumbo.Jet;
using System.Threading;
using System.IO;
using Tkl.Jumbo.CommandLine;

namespace JetShell.Commands
{
    // Note: this class needs special handling, don't create it using CommandLineParser.
    [ShellCommand("job"), Description("Runs a job on the Jumbo Jet cluster.")]
    class RunJobCommand : JetShellCommand
    {
        private const int _jobStatusPollInterval = 5000;

        private readonly string[] _args;
        private readonly int _argIndex;

        public RunJobCommand(string[] args, int argIndex)
        {
            _args = args;
            _argIndex = argIndex;
        }

        public override void Run()
        {
            if( _args.Length - _argIndex == 0 )
                Console.WriteLine("Usage: JetShell.exe job <assemblyName> [jobName] [job arguments...]");
            else
            {
                string assemblyFileName = _args[_argIndex];
                Assembly assembly = Assembly.LoadFrom(assemblyFileName);
                if( _args.Length - _argIndex == 1 )
                {
                    Console.WriteLine("Usage: JetShell.exe job <assemblyName> <jobName> [job arguments...]");
                    Console.WriteLine();
                    PrintAssemblyJobList(assembly);
                }
                else
                {
                    string jobName = _args[_argIndex + 1];
                    JobRunnerInfo jobRunnerInfo = JobRunnerInfo.GetJobRunner(assembly, jobName);
                    if( jobRunnerInfo == null )
                    {
                        Console.WriteLine(string.Format("Job {0} does not exist in the assembly {1}.", jobName, Path.GetFileName(assemblyFileName)).SplitLines(Console.WindowWidth - 1, 0));
                        PrintAssemblyJobList(assembly);
                    }
                    else
                    {
                        IJobRunner jobRunner = jobRunnerInfo.CreateInstance(_args, _argIndex + 2);
                        if( jobRunner == null )
                        {
                            string baseUsage = string.Format("Usage: JetShell.exe job {0} {1} ", Path.GetFileName(assemblyFileName), jobRunnerInfo.Name);
                            Console.WriteLine(jobRunnerInfo.CommandLineParser.GetCustomUsage(baseUsage, Console.WindowWidth - 1));
                        }
                        else
                        {
                            Guid jobId = jobRunner.RunJob();
                            bool success = WaitForJobCompletion(JetClient, _jobStatusPollInterval, jobId);
                            jobRunner.FinishJob(success);
                        }
                    }
                }
            }
        }

        private static bool WaitForJobCompletion(JetClient jetClient, int interval, Guid jobId)
        {
            JobStatus status = null;
            do
            {
                Thread.Sleep(interval);
                status = jetClient.JobServer.GetJobStatus(jobId);
                Console.WriteLine(status);
            } while( !status.IsFinished );

            Console.WriteLine();
            if( status.IsSuccessful )
                Console.WriteLine("Job completed.");
            else
                Console.WriteLine("Job failed.");
            Console.WriteLine("Start time: {0:yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fff}", status.StartTime.ToLocalTime());
            Console.WriteLine("End time:   {0:yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fff}", status.EndTime.ToLocalTime());
            TimeSpan duration = status.EndTime - status.StartTime;
            Console.WriteLine("Duration:   {0} ({1}s)", duration, duration.TotalSeconds);

            return status.IsSuccessful;
        }

        private static void PrintAssemblyJobList(Assembly assembly)
        {
            JobRunnerInfo[] jobs = JobRunnerInfo.GetJobRunners(assembly);
            Console.Write(string.Format("The assembly {0} defines the following jobs:", assembly.GetName().Name).SplitLines(Console.WindowWidth - 1, 0));
            Console.WriteLine();
            foreach( JobRunnerInfo job in jobs )
            {
                Console.Write(string.Format("{0,13} : {1}", job.Name, job.Description).SplitLines(Console.WindowWidth - 1, 16));
                Console.WriteLine();
            }
        }
    }
}
