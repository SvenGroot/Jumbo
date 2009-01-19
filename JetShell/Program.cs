using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using System.Net.Sockets;

namespace JetShell
{
    static class Program
    {
        private static readonly Dictionary<string, Action<IJobServerClientProtocol, string[]>> _commands = CreateCommandList();

        public static void Main(string[] args)
        {
            if( args.Length == 0 )
                PrintUsage();
            else
            {
                Action<IJobServerClientProtocol, string[]> commandMethod;
                if( _commands.TryGetValue(args[0], out commandMethod) )
                {
                    try
                    {
                        commandMethod(JetClient.CreateJobServerClient(), args);
                    }
                    catch( SocketException ex )
                    {
                        Console.WriteLine("An error occurred communicating with the server:");
                        Console.WriteLine(ex.Message);
                    }
                    catch( ArgumentException ex )
                    {
                        Console.WriteLine(ex.Message);
                    }
                    catch( InvalidOperationException ex )
                    {
                        Console.WriteLine("Invalid operation:");
                        Console.WriteLine(ex.Message);
                    }
                }
                else
                    PrintUsage();
            }
        }

        private static void PrintMetrics(IJobServerClientProtocol jobServer, string[] args)
        {
            JetMetrics metrics = jobServer.GetMetrics();
            metrics.PrintMetrics(Console.Out);
        }

        private static void PrintUsage()
        {
            // TODO: Write real usage info.
            Console.WriteLine("Invalid command line.");
        }

        private static Dictionary<string, Action<IJobServerClientProtocol, string[]>> CreateCommandList()
        {
            Dictionary<string, Action<IJobServerClientProtocol, string[]>> commands = new Dictionary<string, Action<IJobServerClientProtocol, string[]>>();

            commands.Add("metrics", PrintMetrics);

            return commands;
        }    
    }
}
