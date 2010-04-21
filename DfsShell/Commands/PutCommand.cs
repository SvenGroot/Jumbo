// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.CommandLine;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.IO;
using Tkl.Jumbo.Dfs;

namespace DfsShell.Commands
{
    [ShellCommand("put"), Description("Stores a file or directory on the DFS.")]
    class PutCommand : DfsShellCommandWithProgress
    {
        private readonly string _localPath;
        private readonly string _dfsPath;

        public PutCommand([Description("The path of the local file or directory to upload.")] string localPath,
                              [Description("The path of the DFS file or directory to upload to.")] string dfsPath)
        {
            if( localPath == null )
                throw new ArgumentNullException("localPath");
            if( dfsPath == null )
                throw new ArgumentNullException("dfsPath");

            _localPath = localPath;
            _dfsPath = dfsPath;
        }

        [NamedCommandLineArgument("b"), Description("The block size of the DFS file.")]
        public int BlockSize { get; set; }

        [NamedCommandLineArgument("r"), Description("The replication factor of the DFS file.")]
        public int ReplicationFactor { get; set; }

        [NamedCommandLineArgument("q"), Description("Suppress progress information output.")]
        public bool Quiet { get; set; }

        public override void Run()
        {
            if( !File.Exists(_localPath) && !Directory.Exists(_localPath) )
                Console.Error.WriteLine("Local path {0} does not exist.", _localPath);
            else
            {
                ProgressCallback progressCallback = Quiet ? null : new ProgressCallback(PrintProgress);
                try
                {
                    bool isDirectory = Directory.Exists(_localPath);
                    if( isDirectory )
                    {
                        if( !Quiet )
                            Console.WriteLine("Copying local directory \"{0}\" to DFS directory \"{1}\"...", _localPath, _dfsPath);
                        Client.UploadDirectory(_localPath, _dfsPath, BlockSize, ReplicationFactor, progressCallback);
                    }
                    else
                    {
                        DfsDirectory dir = Client.NameServer.GetDirectoryInfo(_dfsPath);
                        string dfsPath = _dfsPath;
                        if( dir != null )
                        {
                            string fileName = Path.GetFileName(_localPath);
                            dfsPath = DfsPath.Combine(dfsPath, fileName);
                        }
                        if( !Quiet )
                            Console.WriteLine("Copying local file \"{0}\" to DFS file \"{1}\"...", _localPath, dfsPath);
                        Client.UploadFile(_localPath, dfsPath, BlockSize, ReplicationFactor, progressCallback);
                    }
                    if( !Quiet )
                        Console.WriteLine();
                }
                catch( UnauthorizedAccessException ex )
                {
                    Console.Error.WriteLine("Unable to open local file:");
                    Console.Error.WriteLine(ex.Message);
                }
                catch( IOException ex )
                {
                    Console.Error.WriteLine("Unable to read local file:");
                    Console.Error.WriteLine(ex.Message);
                }
            }
        }
    }
}
