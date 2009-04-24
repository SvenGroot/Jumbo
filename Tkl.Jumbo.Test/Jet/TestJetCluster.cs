using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using TaskServerApplication;
using Tkl.Jumbo.Jet;
using JobServerApplication;
using System.IO;
using Tkl.Jumbo.Dfs;
using System.Diagnostics;

namespace Tkl.Jumbo.Test.Jet
{
    class TestJetCluster
    {
        public const int JobServerPort = 11000;
        public const int TaskServerPort = 11001;
        public const int TaskServerFileServerPort = 11002;

        private string _path;
        private Dfs.TestDfsCluster _dfsCluster;

        private Thread _taskServerThread;

        public TestJetCluster(int? blockSize, bool eraseExistingData, int maxTasks, CompressionType compressionType)
        {
            // We can't run more than one TaskServer because they are single instance.
            _dfsCluster = new Tkl.Jumbo.Test.Dfs.TestDfsCluster(1, 1, blockSize, eraseExistingData);
            INameServerClientProtocol nameServer = DfsClient.CreateNameServerClient(Dfs.TestDfsCluster.CreateClientConfig());
            nameServer.WaitForSafeModeOff(Timeout.Infinite);

            _path = Utilities.TestOutputPath; // The DFS cluster will have made sure this path is created.

            JetConfiguration jetConfig = new JetConfiguration();
            jetConfig.JobServer.HostName = "localhost";
            jetConfig.JobServer.Port = JobServerPort;
            if( Environment.OSVersion.Platform == PlatformID.Unix )
                jetConfig.JobServer.ListenIPv4AndIPv6 = false;
            jetConfig.TaskServer.Port = TaskServerPort;
            jetConfig.TaskServer.TaskDirectory = Path.Combine(_path, "TaskServer");
            jetConfig.TaskServer.MaxTasks = maxTasks;
            jetConfig.TaskServer.MaxNonInputTasks = maxTasks;
            jetConfig.TaskServer.FileServerPort = TaskServerFileServerPort;
            jetConfig.FileChannel.CompressionType = compressionType;
            if( Environment.OSVersion.Platform == PlatformID.Unix )
                jetConfig.TaskServer.ListenIPv4AndIPv6 = false;
            DfsConfiguration dfsConfig = Dfs.TestDfsCluster.CreateClientConfig();

            Utilities.TraceLineAndFlush("Jet cluster starting.");

            JobServer.Run(jetConfig, dfsConfig);
            _taskServerThread = new Thread(() => TaskServerThread(jetConfig, dfsConfig));
            _taskServerThread.Name = "TaskServer";
            _taskServerThread.Start();

            Thread.Sleep(1000);
            Utilities.TraceLineAndFlush("Jet cluster started.");
        }

        public void Shutdown()
        {
            Utilities.TraceLineAndFlush("Jet cluster shutting down.");
            TaskServer.Shutdown();
            _taskServerThread.Join();
            JobServer.Shutdown();
            _dfsCluster.Shutdown();
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
