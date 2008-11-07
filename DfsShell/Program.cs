using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.Net.Sockets;
using IO = System.IO;

namespace DfsShell
{
    static class Program
    {
        private static Dictionary<string, Action<INameServerClientProtocol, string[]>> _commands = CreateCommandList();

        public static void Main(string[] args)
        {
            if( args.Length == 0 )
                PrintUsage();
            else
            {
                Action<INameServerClientProtocol, string[]> commandMethod;
                if( _commands.TryGetValue(args[0], out commandMethod) )
                {
                    try
                    {
                        commandMethod(DfsClient.CreateNameServerClient(), args);
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

            Console.ReadKey();
        }

        private static void CreateDirectory(INameServerClientProtocol nameServer, string[] args)
        {
            if( args.Length != 2 )
                Console.WriteLine("Usage: DfsShell mkdir <path>");
            else
            {
                nameServer.CreateDirectory(args[1]);
            }
        }

        private static void ListDirectory(INameServerClientProtocol nameServer, string[] args)
        {
            if( args.Length > 2 )
                Console.WriteLine("Usage: DfsShell ls <path>");
            else
            {
                string path = args.Length == 2 ? args[1] : "/";
                Directory dir = nameServer.GetDirectoryInfo(path);
                if( dir == null )
                    Console.WriteLine("Directory not found.");
                else
                    dir.PrintListing(Console.Out);
            }
        }

        private static void PutFile(INameServerClientProtocol nameServer, string[] args)
        {
            if( args.Length != 3 )
                Console.WriteLine("Usage: DfsShell put <local file> <path>");
            else
            {
                string localPath = args[1];
                string dfsPath = args[2];
                if( !IO.File.Exists(localPath) )
                    Console.WriteLine("Local file {0} does not exist.", localPath);
                else
                {
                    Directory dir = nameServer.GetDirectoryInfo(dfsPath);
                    if( dir != null )
                    {
                        string fileName = IO.Path.GetFileName(args[1]);
                        if( !dfsPath.EndsWith(FileSystemEntry.DirectorySeparator.ToString()) )
                            dfsPath += FileSystemEntry.DirectorySeparator;
                        dfsPath += fileName;
                    }
                    try
                    {
                        using( IO.FileStream inputStream = IO.File.OpenRead(localPath) )
                        using( DfsOutputStream outputStream = new DfsOutputStream(nameServer, dfsPath) )
                        {
                            Console.WriteLine("Writing local \"{0}\" to dfs \"{1}\"...", localPath, dfsPath);
                            CopyStream(inputStream, outputStream);
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

        private static void GetFile(INameServerClientProtocol nameServer, string[] args)
        {
            if( args.Length < 2 || args.Length > 3 )
                Console.WriteLine("Usage: DfsShell get <path> [local path]");
            else
            {
                string dfsPath = args[1];
                string localPath;
                if( args.Length == 3 )
                    localPath = args[2];
                else
                {
                    int index = dfsPath.LastIndexOf(FileSystemEntry.DirectorySeparator);
                    if( index < 0 || index + 1 >= dfsPath.Length )
                    {
                        Console.WriteLine("Invalid dfs path.");
                        return;
                    }
                    localPath = dfsPath.Substring(index + 1);
                }
                localPath = IO.Path.Combine(Environment.CurrentDirectory, localPath);
                try
                {
                    using( DfsInputStream inputStream = new DfsInputStream(nameServer, dfsPath) )
                    using( IO.FileStream outputStream = IO.File.Create(localPath) )
                    {
                        Console.WriteLine("Writing dfs \"{0}\" to local \"{1}\"...", dfsPath, localPath);
                        CopyStream(inputStream, outputStream);
                    }
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

        private static void Delete(INameServerClientProtocol nameServer, string[] args)
        {
            if( args.Length != 2 )
                Console.WriteLine("Usage: DfsShell rm <path>");
            else
            {
                Delete(nameServer, args[1], false);
            }
        }

        private static void DeleteRecursive(INameServerClientProtocol nameServer, string[] args)
        {
            if( args.Length != 2 )
                Console.WriteLine("Usage: DfsShell rmr <path>");
            else
            {
                Delete(nameServer, args[1], true);
            }
        }

        private static void Delete(INameServerClientProtocol nameServer, string path, bool recursive)
        {
            nameServer.Delete(path, recursive);
        }

        private static void PrintFileInfo(INameServerClientProtocol nameServer, string[] args)
        {
            if( args.Length != 2 )
                Console.WriteLine("Usage: DfsShell fileinfo <path>");
            else
            {
                string path = args[1];
                File file = nameServer.GetFileInfo(path);
                if( file == null )
                    Console.WriteLine("File not found.");
                else
                    file.PrintFileInfo(Console.Out);
            }
        }

        private static void PrintBlockInfo(INameServerClientProtocol nameServer, string[] args)
        {
            if( args.Length != 2 )
                Console.WriteLine("Usage: DfsShell blockinfo <block id>");
            else
            {
                try
                {
                    Guid blockID = new Guid(args[1]);
                    ServerAddress[] servers = nameServer.GetDataServersForBlock(blockID);
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

        private static void PrintMetrics(INameServerClientProtocol nameServer, string[] args)
        {
            DfsMetrics metrics = nameServer.GetMetrics();
            metrics.PrintMetrics(Console.Out);
        }

        private static Dictionary<string, Action<INameServerClientProtocol, string[]>> CreateCommandList()
        {
            Dictionary<string, Action<INameServerClientProtocol, string[]>> result = new Dictionary<string, Action<INameServerClientProtocol, string[]>>();

            result.Add("mkdir", CreateDirectory);
            result.Add("ls", ListDirectory);
            result.Add("put", PutFile);
            result.Add("get", GetFile);
            result.Add("rm", Delete);
            result.Add("rmr", DeleteRecursive);
            result.Add("fileinfo", PrintFileInfo);
            result.Add("blockinfo", PrintBlockInfo);
            result.Add("metrics", PrintMetrics);

            return result;
        }

        private static void PrintUsage()
        {
            // TODO: Write real usage info.
            Console.WriteLine("Invalid command line.");
        }

        private static void CopyStream(IO.Stream inputStream, IO.Stream outputStream)
        {
            byte[] buffer = new byte[4096];
            int bytesRead;
            int prevPercentage = -1;
            while( (bytesRead = inputStream.Read(buffer, 0, buffer.Length)) != 0 )
            {
                int percentage = (int)((inputStream.Position / (float)inputStream.Length) * 100);
                if( percentage > prevPercentage )
                {
                    prevPercentage = percentage;
                    Console.Write("\r{0}%", percentage);
                }
                outputStream.Write(buffer, 0, bytesRead);
            }
            Console.WriteLine();
        }
    
    }
}
