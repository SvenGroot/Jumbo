// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using TaskServerApplication;
using Ookii.Jumbo.Jet;
using JobServerApplication;
using System.IO;
using Ookii.Jumbo.Dfs;
using System.Diagnostics;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Test.Jet
{
    public class TestJetCluster
    {
        public const int JobServerPort = 11000;
        public const int TaskServerPort = 11001;
        public const int TaskServerFileServerPort = 11002;

        private readonly FileSystemClient _fileSystemClient;
        private readonly JetClient _jetClient;

        private string _path;
        private string _localFsRoot;
        private Dfs.TestDfsCluster _dfsCluster;

        private Thread _taskServerThread;

        public TestJetCluster(int? blockSize, bool eraseExistingData, int taskSlots, CompressionType compressionType, bool localFs = false)
        {
            // We can't run more than one TaskServer because they are single instance.
            if( !localFs )
            {
                _dfsCluster = new Ookii.Jumbo.Test.Dfs.TestDfsCluster(1, 1, blockSize, eraseExistingData);
                _dfsCluster.Client.WaitForSafeModeOff(Timeout.Infinite);
                _fileSystemClient = _dfsCluster.Client;                
            }
            else
            {
                log4net.LogManager.ResetConfiguration();
                log4net.Config.BasicConfigurator.Configure();
                Utilities.TraceLineAndFlush("Jet cluster using local file system.");
            }

            _path = Utilities.TestOutputPath; // The DFS cluster will have made sure this path is created.
            if( localFs )
            {
                if( eraseExistingData && System.IO.Directory.Exists(_path) )
                    System.IO.Directory.Delete(_path, true);
                System.IO.Directory.CreateDirectory(_path);
                _localFsRoot = Path.Combine(_path, "FileSystem");
                Directory.CreateDirectory(_localFsRoot);
                _fileSystemClient = new LocalFileSystemClient(_localFsRoot);
            }

            JetConfiguration jetConfig = new JetConfiguration();
            jetConfig.JobServer.HostName = "localhost";
            jetConfig.JobServer.Port = JobServerPort;
            if( Environment.OSVersion.Platform == PlatformID.Unix )
                jetConfig.JobServer.ListenIPv4AndIPv6 = false;
            jetConfig.TaskServer.Port = TaskServerPort;
            jetConfig.TaskServer.TaskDirectory = Path.Combine(_path, "TaskServer");
            jetConfig.TaskServer.TaskSlots = taskSlots;
            jetConfig.TaskServer.FileServerPort = TaskServerFileServerPort;
            jetConfig.FileChannel.CompressionType = compressionType;
            jetConfig.FileChannel.DeleteIntermediateFiles = false;
            if( Environment.OSVersion.Platform == PlatformID.Unix )
                jetConfig.TaskServer.ListenIPv4AndIPv6 = false;
            DfsConfiguration dfsConfig = localFs ? new LocalFileSystemClient(_localFsRoot).Configuration : Dfs.TestDfsCluster.CreateClientConfig();
            //jetConfig.FileChannel.DeleteIntermediateFiles = false;

            Utilities.TraceLineAndFlush("Jet cluster starting.");

            JobServer.Run(new JumboConfiguration(), jetConfig, dfsConfig);
            _taskServerThread = new Thread(() => TaskServerThread(jetConfig, dfsConfig));
            _taskServerThread.Name = "TaskServer";
            _taskServerThread.Start();

            Thread.Sleep(1000);
            Utilities.TraceLineAndFlush("Jet cluster started.");
            _jetClient = new JetClient(CreateClientConfig());
        }

        public FileSystemClient FileSystemClient
        {
            get { return _fileSystemClient; }
        }


        public JetClient JetClient
        {
            get { return _jetClient; }
        }

        public void Shutdown()
        {
            Utilities.TraceLineAndFlush("Jet cluster shutting down.");
            TaskServer.Shutdown();
            _taskServerThread.Join();
            JobServer.Shutdown();
            if( _dfsCluster != null )
                _dfsCluster.Shutdown();
            Thread.Sleep(5000);
            Utilities.TraceLineAndFlush("Jet cluster shutdown complete.");
        }

        public static JetConfiguration CreateClientConfig()
        {
            JetConfiguration config = new JetConfiguration();
            config.JobServer.HostName = "localhost";
            config.JobServer.Port = JobServerPort;
            config.TaskServer.Port = TaskServerPort;
            return config;
        }

        private void TaskServerThread(JetConfiguration jetConfig, DfsConfiguration dfsConfig)
        {
            TaskServer.Run(jetConfig, dfsConfig);
        }
    }
}
