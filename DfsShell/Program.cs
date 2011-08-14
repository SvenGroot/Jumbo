// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Net.Sockets;
using IO = System.IO;
using System.Threading;
using Tkl.Jumbo;
using Ookii.CommandLine;
using System.Reflection;
using DfsShell.Commands;

namespace DfsShell
{
    static class Program
    {
        //private static readonly Dictionary<string, Action<DfsClient, string[]>> _commands = CreateCommandList();

        public static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();
            if( args.Length == 0 )
                PrintUsage();
            else
            {
                string commandName = args[0].Trim();
                Type commandType = ShellCommand.GetShellCommand(Assembly.GetExecutingAssembly(), commandName);

                if( commandType != null )
                {
                    DfsShellCommand command = null;
                    // DFS paths use / as the directory separator, so use - even on Windows.
                    CommandLineParser parser = new CommandLineParser(commandType, new[] { "-" });
                    try
                    {
                        command = (DfsShellCommand)parser.Parse(args, 1);
                    }
                    catch( CommandLineArgumentException ex )
                    {
                        Console.Error.WriteLine(ex.Message);
                        Console.WriteLine();
                    }

                    if( command == null )
                        parser.WriteUsageToConsole(new WriteUsageOptions() { UsagePrefix = CommandLineParser.DefaultUsagePrefix + " " + commandName.ToLowerInvariant() });
                    else
                    {
                        try
                        {
                            command.Client = new DfsClient();
                            command.Run();
                        }
                        catch( SocketException ex )
                        {
                            Console.Error.WriteLine("An error occurred communicating with the server:");
                            Console.Error.WriteLine(ex.Message);
                        }
                        catch( DfsException ex )
                        {
                            Console.Error.WriteLine("An error occurred processing the command:");
                            Console.Error.WriteLine(ex.Message);
                        }
                        catch( ArgumentException ex )
                        {
                            Console.Error.WriteLine(ex.Message);
                        }
                        catch( InvalidOperationException ex )
                        {
                            Console.Error.WriteLine("Invalid operation:");
                            Console.Error.WriteLine(ex.Message);
                        }
                    }
                }
                else
                    PrintUsage();
            }
        }

        private static void PrintVersion(DfsClient client, string[] args)
        {
            Console.WriteLine("Jumbo {0}", typeof(Tkl.Jumbo.ServerAddress).Assembly.GetName().Version);
        }

        private static void PrintRevision(DfsClient client, string[] args)
        {
            Console.WriteLine(typeof(Tkl.Jumbo.ServerAddress).Assembly.GetName().Version.Revision);
        }

        private static void PrintUsage()
        {
            using( LineWrappingTextWriter writer = LineWrappingTextWriter.ForConsoleOut() )
            {
                writer.WriteLine("Usage: DfsShell <command> [args...]");
                writer.WriteLine();
                writer.WriteLine("The following commands are available:");
                writer.WriteLine();
                ShellCommand.WriteAssemblyCommandList(writer, Assembly.GetExecutingAssembly());
            }
        }
    }
}
