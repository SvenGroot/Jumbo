using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using NUnit.Framework;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Test.Tasks;
using System.Threading;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    [Category("JetClusterTest")]
    public class JobAndTaskServerTests
    {
        private enum TaskKind
        {
            Pull,
            Push,
            Merge
        }

        private TestJetCluster _cluster;
        private const string _fileName = "/jobinput.txt";
        private int _lines;

        [TestFixtureSetUp]
        public void Setup()
        {
            _cluster = new TestJetCluster(16777216, true, 2);
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            const int size = 50000000;
            using( DfsOutputStream stream = dfsClient.CreateFile(_fileName) )
            {
                _lines = Utilities.GenerateDataLines(stream, size);
            }
            Utilities.TraceLineAndFlush("File generation complete.");
        }

        [TestFixtureTearDown]
        public void TearDown()
        {
            _cluster.Shutdown();
        }

        [Test]
        public void TestJobExecution()
        {
            RunJob(false, "/joboutput", TaskKind.Pull, ChannelType.File);
        }

        [Test]
        public void TestJobExecutionTcpFileDownload()
        {
            RunJob(true, "/joboutput2", TaskKind.Pull, ChannelType.File);
        }

        [Test]
        public void TestJobExecutionPushTask()
        {
            RunJob(false, "/joboutput3", TaskKind.Push, ChannelType.File);
        }

        [Test]
        public void TestJobExecutionPipelineChannel()
        {
            RunJob(false, "/joboutput4", TaskKind.Push, ChannelType.Pipeline);
        }

        [Test]
        public void TestJobExecutionMergeTask()
        {
            RunJob(false, "/joboutput5", TaskKind.Merge, ChannelType.File);
        }

        private void RunJob(bool forceFileDownload, string outputPath, TaskKind taskKind, ChannelType channelType)
        {
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            dfsClient.NameServer.CreateDirectory(outputPath);

            Type counterTask = null;
            Type adderTask = null;
            switch( taskKind )
            {
            case TaskKind.Pull:
                counterTask = typeof(LineCounterTask);
                adderTask = typeof(LineAdderTask);
                break;
            case TaskKind.Push:
                counterTask = typeof(LineCounterPushTask);
                adderTask = typeof(LineAdderPushTask);
                break;
            case TaskKind.Merge:
                counterTask = typeof(LineCounterTask);
                adderTask = typeof(LineAdderMergeTask);
                break;
            }

            Tkl.Jumbo.Dfs.File file = dfsClient.NameServer.GetFileInfo(_fileName);
            JobConfiguration config = CreateConfiguration(dfsClient, file, outputPath, forceFileDownload, counterTask, adderTask, channelType);

            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, dfsClient, typeof(LineCounterTask).Assembly.Location);

            bool complete = target.JobServer.WaitForJobCompletion(job.JobID, Timeout.Infinite);
            Assert.IsTrue(complete);

            string outputFileName = DfsPath.Combine(outputPath, "OutputTask001");

            using( DfsInputStream stream = dfsClient.OpenFile(outputFileName) )
            using( StreamReader reader = new StreamReader(stream) )
            {
                // The test merge task writes the number of inputs it received to the file.
                if( taskKind == TaskKind.Merge )
                    Assert.AreEqual(file.Blocks.Count, Convert.ToInt32(reader.ReadLine()));

                Assert.AreEqual(_lines, Convert.ToInt32(reader.ReadLine()));
            }

            Console.WriteLine(config);
        }

        private static JobConfiguration CreateConfiguration(DfsClient dfsClient, Tkl.Jumbo.Dfs.File file, string outputPath, bool forceFileDownload, Type counterTask, Type adderTask, ChannelType channelType)
        {

            JobConfiguration config = new JobConfiguration(System.IO.Path.GetFileName(typeof(LineCounterTask).Assembly.Location));

            config.AddInputStage("Task", file, counterTask, typeof(LineRecordReader));
            string stage = "Task";
            if( channelType == ChannelType.Pipeline )
            {
                // Pipeline channel cannot merge so we will add another stage in between.
                config.AddPointToPointStage("IntermediateTask", "Task", adderTask, ChannelType.Pipeline, null, null, null);
                stage = "IntermediateTask";
            }
            config.AddStage("OutputTask", new[] { stage }, adderTask, 1, ChannelType.File, null, outputPath, typeof(TextRecordWriter<Int32Writable>));
            config.Channels[0].ForceFileDownload = forceFileDownload;

            return config;
        }
    }
}
