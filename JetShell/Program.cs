using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using System.Net.Sockets;
using System.Reflection;
using Tkl.Jumbo.Jet.Jobs;
using System.IO;

namespace JetShell
{
    static class Program
    {
        private static readonly Dictionary<string, Action<JetClient, string[]>> _commands = CreateCommandList();
        private const int _jobStatusPollInterval = 5000;

        public static void Main(string[] args)
        {
            log4net.Config.BasicConfigurator.Configure();
            log4net.LogManager.GetRepository().Threshold = log4net.Core.Level.Info;

            if( args.Length == 0 )
                PrintUsage();
            else
            {
                Action<JetClient, string[]> commandMethod;
                if( _commands.TryGetValue(args[0], out commandMethod) )
                {
                    try
                    {
                        commandMethod(new JetClient(), args);
                    }
                    catch( SocketException ex )
                    {
                        Console.WriteLine("An error occurred communicating with the server:");
                        Console.WriteLine(ex.Message);
                    }
                    catch( InvalidOperationException ex )
                    {
                        Console.WriteLine("Invalid operation:");
                        Console.WriteLine(ex.Message);
                    }
                    catch( Exception ex )
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                else
                    PrintUsage();
            }
        }

        private static void PrintMetrics(JetClient jetClient, string[] args)
        {
            JetMetrics metrics = jetClient.JobServer.GetMetrics();
            metrics.PrintMetrics(Console.Out);
        }

        private static void RunJob(JetClient jetClient, string[] args)
        {
            if( args.Length < 2 )
                Console.WriteLine("Usage: JetShell.exe job <assemblyName> [jobName] [job arguments...]");
            else
            {
                string assemblyFileName = args[1];
                Assembly assembly = Assembly.LoadFrom(assemblyFileName);
                if( args.Length == 2 )
                {
                    PrintAssemblyJobList(assembly);
                }
                else
                {
                    string jobName = args[2];
                    JobRunnerInfo jobRunnerInfo = JobRunnerInfo.GetJobRunner(assembly, jobName);
                    if( jobRunnerInfo == null )
                    {
                        Console.WriteLine("Job {0} does not exist in the assembly {1}.", jobName, assemblyFileName);
                        PrintAssemblyJobList(assembly);
                    }
                    else
                    {
                        string[] remainingArgs = new string[args.Length - 3];
                        if( remainingArgs.Length > 0 )
                            Array.Copy(args, 3, remainingArgs, 0, remainingArgs.Length);
                        IJobRunner jobRunner = jobRunnerInfo.CreateInstance(remainingArgs);
                        if( jobRunner == null )
                        {
                            Console.WriteLine("Usage: JetShell.exe job {0} {1}", assemblyFileName, jobRunnerInfo.Usage);
                        }
                        else
                        {
                            Guid jobId = jobRunner.RunJob();
                            WaitForJobCompletion(jetClient, _jobStatusPollInterval, jobId);
                            jobRunner.FinishJob();
                        }
                    }
                }
            }
        }

        private static void WaitForJobCompletion(JetClient jetClient, int interval, Guid jobId)
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
        }

        private static void PrintAssemblyJobList(Assembly assembly)
        {
            JobRunnerInfo[] jobs = JobRunnerInfo.GetJobRunners(assembly);
            Console.WriteLine("The specified assembly defines the following jobs:");
            Console.WriteLine();
            foreach( JobRunnerInfo job in jobs )
            {
                Console.WriteLine("{0}: {1}", job.Name, job.Description);
                Console.WriteLine();
            }
        }

        private static void PrintUsage()
        {
            // TODO: Write real usage info.
            Console.WriteLine("Invalid command line.");
        }

        private static Dictionary<string, Action<JetClient, string[]>> CreateCommandList()
        {
            Dictionary<string, Action<JetClient, string[]>> commands = new Dictionary<string, Action<JetClient, string[]>>();

            commands.Add("metrics", PrintMetrics);
            commands.Add("job", RunJob);

            return commands;
        }    
    }
}
