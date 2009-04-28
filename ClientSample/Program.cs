using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Runtime.Remoting;
using System.Net.Sockets;
using System.Net;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using System.Diagnostics;
using Tkl.Jumbo.Jet;
using System.Xml.Serialization;
using System.Threading;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;

namespace ClientSample
{
    class Program
    {
        static void Main(string[] args)
        {
            log4net.Config.BasicConfigurator.Configure();
            log4net.LogManager.GetRepository().Threshold = log4net.Core.Level.Info;

            if( args.Length < 2 )
            {
                Console.WriteLine("Usage: ClientSample.exe <task> <inputpath> [aggregate task count] [outputpath]");
                return;
            }

            string task = args[0];
            string input = null;
            int aggregateTaskCount = 0;
            string output = null;
            if( args[0] != "gensort" )
            {
                input = args[1];
                aggregateTaskCount = args.Length >= 3 ? Convert.ToInt32(args[2]) : 1;
                output = args.Length >= 4 ? args[3] : "/output";
            }

            DfsClient dfsClient = new DfsClient();
            JetClient jetClient = new JetClient();
            switch( task )
            {
            case "readtest":
                ReadTest(input);
                return;
            default:
                Console.WriteLine("Unknown task.");
                return;
            }
            
        }

        private static void ReadTest(string path)
        {
            Console.WriteLine("Reading file {0}.", path);
            DfsClient client = new DfsClient();
            Stopwatch sw = new Stopwatch();
            sw.Start();
            using( DfsInputStream stream = client.OpenFile(path) )
            {
                const int size = 0x10000;
                byte[] buffer = new byte[size];
                while( stream.Read(buffer, 0, size) > 0 )
                {
                }
            }
            sw.Stop();
            Console.WriteLine("Reading file complete: {0}.", sw.ElapsedMilliseconds / 1000.0f);
        }
    }
}
