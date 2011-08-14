﻿// $Id$
//
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
using Ookii.CommandLine;
using JetShell.Commands;

namespace JetShell
{
    static class Program
    {
        public static int Main(string[] args)
        {
            AssemblyResolver.Register();

            log4net.Config.BasicConfigurator.Configure();
            log4net.LogManager.GetRepository().Threshold = log4net.Core.Level.Info;

            if( args.Length == 0 )
                PrintUsage();
            else
            {
                string commandName = args[0].Trim();
                Type commandType = ShellCommand.GetShellCommand(Assembly.GetExecutingAssembly(), commandName);

                if( commandType != null )
                {
                    JetShellCommand command = null;
                    CommandLineParser parser = null;
                    if( commandType == typeof(RunJobCommand) )
                    {
                        command = new RunJobCommand(args, 1);
                    }
                    else
                    {
                        // DFS paths use / as the directory separator, so use - even on Windows.
                        parser = new CommandLineParser(commandType, new[] { "-" });
                        try
                        {
                            command = (JetShellCommand)parser.Parse(args, 1);
                        }
                        catch( CommandLineArgumentException ex )
                        {
                            Console.Error.WriteLine(ex.Message);
                            Console.WriteLine();
                        }
                    }

                    if( command == null )
                        parser.WriteUsageToConsole(new WriteUsageOptions() { UsagePrefix = CommandLineParser.DefaultUsagePrefix + " " + commandName.ToLowerInvariant() });
                    else
                    {
                        try
                        {
                            command.JetClient = new JetClient();
                            command.Run();
                            return command.ExitCode;
                        }
                        catch( SocketException ex )
                        {
                            Console.Error.WriteLine("An error occurred communicating with the server:");
                            Console.Error.WriteLine(ex.Message);
                        }
                        catch( InvalidOperationException ex )
                        {
                            Console.Error.WriteLine("Invalid operation:");
                            Console.Error.WriteLine(ex.Message);
                        }
                        catch( TargetInvocationException ex )
                        {
                            if( ex.InnerException == null )
                                Console.Error.WriteLine(ex.Message);
                            else
                                Console.Error.WriteLine(ex.InnerException.Message);
                        }
                        catch( Exception ex )
                        {
						  Console.Error.WriteLine(ex.ToString());
                        }
                    }
                }
                else
                    PrintUsage();
            }

            return 1;
        }

        private static void PrintUsage()
        {
            using( LineWrappingTextWriter writer = LineWrappingTextWriter.ForConsoleOut() )
            {
                writer.WriteLine("Usage: JetShell <command> [args...]");
                writer.WriteLine();
                writer.WriteLine("The following commands are available:");
                writer.WriteLine();
                ShellCommand.WriteAssemblyCommandList(writer, Assembly.GetExecutingAssembly());
            }
        }  
    }
}
