using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using Tkl.Jumbo;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Jet.Jobs;
using System.Threading;

namespace JetShell
{
    static class Program
    {
        // This field isn't used but the constructor does the work so
        // it needs to be there.
#pragma warning disable 414
        private static readonly AssemblyResolver _resolver = new AssemblyResolver();
#pragma warning restore 414
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
                    catch( TargetInvocationException ex )
                    {
                        if( ex.InnerException == null )
                            Console.WriteLine(ex.Message);
                        else
                            Console.WriteLine(ex.InnerException.Message);
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
                    Console.WriteLine("Usage: JetShell.exe job <assemblyName> <jobName> [job arguments...]");
                    Console.WriteLine();
                    PrintAssemblyJobList(assembly);
                }
                else
                {
                    string jobName = args[2];
                    JobRunnerInfo jobRunnerInfo = JobRunnerInfo.GetJobRunner(assembly, jobName);
                    if( jobRunnerInfo == null )
                    {
                        Console.WriteLine(string.Format("Job {0} does not exist in the assembly {1}.", jobName, Path.GetFileName(assemblyFileName)).GetLines(Console.WindowWidth - 1, 0));
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
                            string baseUsage = string.Format("Usage: JetShell.exe job {0} ", Path.GetFileName(assemblyFileName));
                            Console.WriteLine(jobRunnerInfo.GetUsage(baseUsage, Console.WindowWidth - 1));
                        }
                        else
                        {
                            Guid jobId = jobRunner.RunJob();
                            bool success = WaitForJobCompletion(jetClient, _jobStatusPollInterval, jobId);
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
            Console.Write(string.Format("The assembly {0} defines the following jobs:", assembly.GetName().Name).GetLines(Console.WindowWidth - 1, 0));
            Console.WriteLine();
            foreach( JobRunnerInfo job in jobs )
            {
                Console.Write(string.Format("{0,13} : {1}", job.Name, job.Description).GetLines(Console.WindowWidth - 1, 16));
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
