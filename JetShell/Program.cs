// $Id$
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
using Tkl.Jumbo.CommandLine;
using JetShell.Commands;

namespace JetShell
{
    static class Program
    {
        // This field isn't used but the constructor does the work so
        // it needs to be there.
#pragma warning disable 414
        private static readonly AssemblyResolver _resolver = new AssemblyResolver();
#pragma warning restore 414

        public static void Main(string[] args)
        {
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
                        parser = new CommandLineParser(commandType);
                        parser.NamedArgumentSwitch = "-"; // DFS paths use / as the directory separator, so use - even on Windows.
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
                        Console.WriteLine(parser.GetCustomUsage("Usage: DfsShell.exe " + commandName.ToLowerInvariant(), Console.WindowWidth - 1));
                    else
                    {
                        try
                        {
                            command.JetClient = new JetClient();
                            command.Run();
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
                            Console.Error.WriteLine(ex.Message);
                        }
                    }
                }
                else
                    PrintUsage();
            }
        }

        private static void PrintUsage()
        {
            Console.WriteLine("Usage: DfsShell <command> [args...]");
            Console.WriteLine();
            Console.WriteLine("The following commands are available:");
            Console.WriteLine();
            ShellCommand.PrintAssemblyCommandList(Assembly.GetExecutingAssembly());
        }  
    }
}
