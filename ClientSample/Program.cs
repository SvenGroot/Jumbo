// $Id$
//
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
using Tkl.Jumbo.Jet.Samples.IO;
using Tkl.Jumbo.Dfs.FileSystem;

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
                Console.WriteLine("Usage: ClientSample.exe <task> <inputpath> [other arguments]");
                return;
            }

            string task = args[0];
            string input = null;
            input = args[1];

            switch( task )
            {
            case "readtest":
                ReadTest(input);
                return;
            case "checktpch":
                CheckTpcH(input, args[2]);
                break;
            default:
                Console.WriteLine("Unknown task.");
                return;
            }
            
        }

        private static void ReadTest(string path)
        {
            Console.WriteLine("Reading file {0}.", path);
            FileSystemClient client = FileSystemClient.Create();
            Stopwatch sw = new Stopwatch();
            sw.Start();
            using( Stream stream = client.OpenFile(path) )
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

        private static void CheckTpcH(string path, string referencePath)
        {
            FileSystemClient client = FileSystemClient.Create();
            LineItem referenceLineItem = new LineItem();

            JumboDirectory directory = client.GetDirectoryInfo(path);
            var files = from child in directory.Children
                        let file = child as JumboFile
                        where file != null && file.Name.StartsWith("LineItem")
                        orderby file.Name
                        select file;

            int record = 0;
            using( StreamReader reader = File.OpenText(Path.Combine(referencePath, "lineitem.tbl")) )
            {
                foreach( JumboFile file in files )
                {
                    using( Stream dfsStream = client.OpenFile(file.FullPath) )
                    using( RecordFileReader<LineItem> recordReader = new RecordFileReader<LineItem>(dfsStream, 0, dfsStream.Length, true) )
                    {
                        foreach( LineItem item in recordReader.EnumerateRecords() )
                        {
                            ++record;
                            referenceLineItem.FromString(reader.ReadLine());
                            if( !item.Equals(referenceLineItem) )
                            {
                                Console.WriteLine();
                                Console.WriteLine("Record {0} is not a correct (DFS file: {1}, record {2}).", record, file.Name, recordReader.RecordsRead);
                                Console.ReadKey();
                                return;
                            }
                            if( record % 1000 == 0 )
                                Console.Write("\r{0}", record);
                        }
                    }
                }
            }
            Console.WriteLine();
            Console.WriteLine("Total records: {0}", record);
        }
    }
}
