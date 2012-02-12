﻿// $Id$
//
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
using Ookii.CommandLine;

namespace JetShell.Commands
{
    // Note: this class needs special handling, don't create it using CommandLineParser.
    [ShellCommand("job"), Description("Runs a job on the Jumbo Jet cluster.")]
    class RunJobCommand : JetShellCommand
    {
        private readonly string[] _args;
        private readonly int _argIndex;

        public RunJobCommand(string[] args, int argIndex)
        {
            _args = args;
            _argIndex = argIndex;
        }

        public override void Run()
        {
            ExitCode = 1; // Assume failure unless we can successfully run a job.
            if( _args.Length - _argIndex == 0 )
                Console.WriteLine("Usage: JetShell.exe job <assemblyName> [jobName] [job arguments...]");
            else
            {
                string assemblyFileName = _args[_argIndex];
                Assembly assembly = Assembly.LoadFrom(assemblyFileName);
                if( _args.Length - _argIndex == 1 )
                {
                    using( LineWrappingTextWriter writer = LineWrappingTextWriter.ForConsoleOut() )
                    {
                        writer.WriteLine("Usage: JetShell.exe job <assemblyName> <jobName> [job arguments...]");
                        writer.WriteLine();
                        PrintAssemblyJobList(writer, assembly);
                    }
                }
                else
                {
                    string jobName = _args[_argIndex + 1];
                    JobRunnerInfo jobRunnerInfo = JobRunnerInfo.GetJobRunner(assembly, jobName);
                    if( jobRunnerInfo == null )
                    {
                        using( LineWrappingTextWriter writer = LineWrappingTextWriter.ForConsoleOut() )
                        {
                            writer.WriteLine("Job {0} does not exist in the assembly {1}.", jobName, Path.GetFileName(assemblyFileName));
                            PrintAssemblyJobList(writer, assembly);
                        }
                    }
                    else
                    {
                        IJobRunner jobRunner = jobRunnerInfo.CreateInstance(_args, _argIndex + 2);
                        if( jobRunner == null )
                        {
                            WriteUsageOptions options = new WriteUsageOptions() { UsagePrefix = string.Format("{0} job {1} {2} ", CommandLineParser.DefaultUsagePrefix, Path.GetFileName(assemblyFileName), jobRunnerInfo.Name) };
                            jobRunnerInfo.CommandLineParser.WriteUsageToConsole(options);
                        }
                        else
                        {
                            Guid jobId = jobRunner.RunJob();
                            if( jobId != Guid.Empty )
                            {
                                bool success = JetClient.WaitForJobCompletion(jobId);
                                jobRunner.FinishJob(success);
                                ExitCode = success ? 0 : 1;
                            }
                            else
                                ExitCode = 2;
                        }
                    }
                }
            }
        }

        private static void PrintAssemblyJobList(TextWriter writer, Assembly assembly)
        {
            LineWrappingTextWriter lineWriter = writer as LineWrappingTextWriter;
            JobRunnerInfo[] jobs = JobRunnerInfo.GetJobRunners(assembly);
            writer.Write("The assembly {0} defines the following jobs:", assembly.GetName().Name);
            writer.WriteLine();
            if( lineWriter != null )
                lineWriter.Indent = 16;
            foreach( JobRunnerInfo job in jobs )
            {
                if( lineWriter != null )
                    lineWriter.ResetIndent();
                writer.WriteLine("{0,13} : {1}", job.Name, job.Description);
                writer.WriteLine();
            }
        }
    }
}
