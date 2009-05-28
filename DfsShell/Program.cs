using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Net.Sockets;
using IO = System.IO;
using System.Threading;
using Tkl.Jumbo;

namespace DfsShell
{
    static class Program
    {
        private static readonly Dictionary<string, Action<DfsClient, string[]>> _commands = CreateCommandList();
        private static string _previousFileName;

        public static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();
            if( args.Length == 0 )
                PrintUsage();
            else
            {
                Action<DfsClient, string[]> commandMethod;
                if( _commands.TryGetValue(args[0], out commandMethod) )
                {
                    try
                    {
                        commandMethod(new DfsClient(), args);
                    }
                    catch( SocketException ex )
                    {
                        Console.WriteLine("An error occurred communicating with the server:");
                        Console.WriteLine(ex.Message);
                    }
                    catch( DfsException ex )
                    {
                        Console.WriteLine("An error occurred processing the command:");
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

        private static void CreateDirectory(DfsClient client, string[] args)
        {
            if( args.Length != 2 )
                Console.WriteLine("Usage: DfsShell mkdir <path>");
            else
            {
                client.NameServer.CreateDirectory(args[1]);
            }
        }

        private static void ListDirectory(DfsClient client, string[] args)
        {
            if( args.Length > 2 )
                Console.WriteLine("Usage: DfsShell ls <path>");
            else
            {
                string path = args.Length == 2 ? args[1] : "/";
                DfsDirectory dir = client.NameServer.GetDirectoryInfo(path);
                if( dir == null )
                    Console.WriteLine("Directory not found.");
                else
                    dir.PrintListing(Console.Out);
            }
        }

        private static void PutFile(DfsClient client, string[] args)
        {
            if( args.Length != 3 )
                Console.WriteLine("Usage: DfsShell put <local path> <dfs path>");
            else
            {
                string localPath = args[1];
                string dfsPath = args[2];
                if( !IO.File.Exists(localPath) && !IO.Directory.Exists(localPath) )
                    Console.WriteLine("Local path {0} does not exist.", localPath);
                else
                {

                    try
                    {
                        bool isDirectory = IO.Directory.Exists(localPath);
                        if( isDirectory )
                        {
                            Console.WriteLine("Copying local directory \"{0}\" to DFS directory \"{1}\"...", localPath, dfsPath);
                            client.UploadDirectory(localPath, dfsPath, PrintProgress);
                        }
                        else
                        {
                            DfsDirectory dir = client.NameServer.GetDirectoryInfo(dfsPath);
                            if( dir != null )
                            {
                                string fileName = IO.Path.GetFileName(args[1]);
                                dfsPath = DfsPath.Combine(dfsPath, fileName);
                            }
                            Console.WriteLine("Copying local file \"{0}\" to DFS file \"{1}\"...", localPath, dfsPath);
                            client.UploadFile(localPath, dfsPath, PrintProgress);
                        }
                    }
                    catch( UnauthorizedAccessException ex )
                    {
                        Console.WriteLine("Unable to open local file:");
                        Console.WriteLine(ex.Message);
                    }
                    catch( IO.IOException ex )
                    {
                        Console.WriteLine("Unable to read local file:");
                        Console.WriteLine(ex.Message);
                    }
                }
            }
        }

        private static void GetFile(DfsClient client, string[] args)
        {
            if( args.Length < 2 || args.Length > 3 )
                Console.WriteLine("Usage: DfsShell get <path> [local path]");
            else
            {
                string dfsPath = args[1];

                FileSystemEntry entry = client.NameServer.GetFileSystemEntryInfo(dfsPath);
                if( entry == null )
                {
                    Console.WriteLine("Path {0} does not exist on the DFS.", dfsPath);
                    return;
                }

                string localPath;
                if( args.Length == 3 )
                    localPath = args[2];
                else
                {
                    localPath = ".";
                }
                localPath = IO.Path.Combine(Environment.CurrentDirectory, localPath);

                try
                {
                    if( entry is DfsFile )
                    {
                        if( IO.Directory.Exists(localPath) )
                        {
                            localPath = IO.Path.Combine(localPath, entry.Name);
                        }
                        Console.WriteLine("Copying DFS file \"{0}\" to local file \"{1}\"...", entry.FullPath, localPath);
                        client.DownloadFile(dfsPath, localPath, PrintProgress);
                    }
                    else
                    {
                        Console.WriteLine("Copying DFS directory \"{0}\" to local directory \"{1}\"...", entry.FullPath, localPath);
                        client.DownloadDirectory(dfsPath, localPath, PrintProgress);
                    }
                    Console.WriteLine();
                }
                catch( UnauthorizedAccessException ex )
                {
                    Console.WriteLine("Unable to open local file:");
                    Console.WriteLine(ex.Message);
                }
                catch( IO.IOException ex )
                {
                    Console.WriteLine("Unable to get file:");
                    Console.WriteLine(ex.Message);
                }
            }
        }

        private static void Delete(DfsClient client, string[] args)
        {
            if( args.Length != 2 )
                Console.WriteLine("Usage: DfsShell rm <path>");
            else
            {
                Delete(client, args[1], false);
            }
        }

        private static void DeleteRecursive(DfsClient client, string[] args)
        {
            if( args.Length != 2 )
                Console.WriteLine("Usage: DfsShell rmr <path>");
            else
            {
                Delete(client, args[1], true);
            }
        }

        private static void Delete(DfsClient client, string path, bool recursive)
        {
            client.NameServer.Delete(path, recursive);
        }

        private static void Move(DfsClient client, string[] args)
        {
            if( args.Length != 3 )
                Console.WriteLine("Usage: DfsShell mv <from> <to>");
            else
            {
                string from = args[1];
                string to = args[2];
                client.NameServer.Move(from, to);
            }
        }

        private static void PrintFileInfo(DfsClient client, string[] args)
        {
            if( args.Length != 2 )
                Console.WriteLine("Usage: DfsShell fileinfo <path>");
            else
            {
                string path = args[1];
                DfsFile file = client.NameServer.GetFileInfo(path);
                if( file == null )
                    Console.WriteLine("File not found.");
                else
                    file.PrintFileInfo(Console.Out);
            }
        }

        private static void PrintBlockInfo(DfsClient client, string[] args)
        {
            if( args.Length != 2 )
                Console.WriteLine("Usage: DfsShell blockinfo <block id>");
            else
            {
                try
                {
                    Guid blockID = new Guid(args[1]);
                    ServerAddress[] servers = client.NameServer.GetDataServersForBlock(blockID);
                    Console.WriteLine("Data server list for block {{{0}}}:", blockID);
                    foreach( ServerAddress server in servers )
                        Console.WriteLine(server);
                }
                catch( FormatException )
                {
                    Console.WriteLine("Invalid guid.");
                }
            }
        }

        private static void PrintMetrics(DfsClient client, string[] args)
        {
            DfsMetrics metrics = client.NameServer.GetMetrics();
            metrics.PrintMetrics(Console.Out);
        }

        private static void PrintFile(DfsClient client, string[] args)
        {
            if( args.Length != 2 )
                Console.WriteLine("Usage: DfsShell cat <path>");
            else
            {
                using( DfsInputStream stream = client.OpenFile(args[1]) )
                using( IO.StreamReader reader = new System.IO.StreamReader(stream) )
                {
                    string line;
                    while( (line = reader.ReadLine()) != null )
                        Console.WriteLine(line);
                }
            }
        }

        private static void PrintSafeMode(DfsClient client, string[] args)
        {
            if( client.NameServer.SafeMode )
                Console.WriteLine("Safe mode is ON.");
            else
                Console.WriteLine("Safe mode is OFF.");
        }

        private static void WaitSafeMode(DfsClient client, string[] args)
        {
            if( args.Length > 2 )
                Console.WriteLine("Usage: DfsShell waitsafemode [timeout]");
            else
            {
                int timeout = Timeout.Infinite;
                if( args.Length == 2 && !Int32.TryParse(args[1], out timeout) )
                    Console.WriteLine("Invalid timeout.");
                else
                {
                    if( client.NameServer.WaitForSafeModeOff(timeout) )
                        Console.WriteLine("Safe mode is OFF.");
                    else
                        Console.WriteLine("Safe mode is ON.");
                }
            }
        }

        private static Dictionary<string, Action<DfsClient, string[]>> CreateCommandList()
        {
            Dictionary<string, Action<DfsClient, string[]>> result = new Dictionary<string, Action<DfsClient, string[]>>();

            result.Add("mkdir", CreateDirectory);
            result.Add("ls", ListDirectory);
            result.Add("put", PutFile);
            result.Add("get", GetFile);
            result.Add("rm", Delete);
            result.Add("rmr", DeleteRecursive);
            result.Add("fileinfo", PrintFileInfo);
            result.Add("blockinfo", PrintBlockInfo);
            result.Add("metrics", PrintMetrics);
            result.Add("safemode", PrintSafeMode);
            result.Add("waitsafemode", WaitSafeMode);
            result.Add("cat", PrintFile);
            result.Add("mv", Move);

            return result;
        }

        private static void PrintUsage()
        {
            // TODO: Write real usage info.
            Console.WriteLine("Invalid command line.");
        }

        private static void PrintProgress(string fileName, int progressPercentage, long progressBytes)
        {
            if( _previousFileName != fileName )
            {
                Console.WriteLine();
                Console.WriteLine("{0}:", fileName);
                _previousFileName = fileName;
            }
            string progressBytesString = progressBytes.ToString("#,0", System.Globalization.CultureInfo.CurrentCulture);
            int width = Console.WindowWidth - 9 - Math.Max(15, progressBytesString.Length);
            int progressWidth = (int)(progressPercentage / 100.0f * width);
            string progressBar = new string('=', progressWidth);;
            if( progressWidth < width )
            {
                progressBar += ">" + new string(' ', width - progressWidth - 1);
            }
            Console.Write("\r{0,3}% [{1}] {2}", progressPercentage, progressBar, progressBytesString);
        }
    }
}
