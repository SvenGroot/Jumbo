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
            RunJob(false, "/joboutput.txt");
        }

        [Test]
        public void TestJobExecutionTcpFileDownload()
        {
            RunJob(true, "/joboutput2.txt");
        }

        private void RunJob(bool forceFileDownload, string outputFileName)
        {
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());

            JobConfiguration config = CreateConfiguration(dfsClient, _fileName, outputFileName, forceFileDownload);

            IJobServerClientProtocol target = JetClient.CreateJobServerClient(TestJetCluster.CreateClientConfig());
            Job job = target.CreateJob();

            using( DfsOutputStream stream = dfsClient.CreateFile(job.JobConfigurationFilePath) )
            {
                config.SaveXml(stream);
            }
            dfsClient.UploadFile(typeof(LineCounterTask).Assembly.Location, DfsPath.Combine(job.Path, "Tkl.Jumbo.Test.Tasks.dll"));

            target.RunJob(job.JobID);
            bool complete = target.WaitForJobCompletion(job.JobID, Timeout.Infinite);
            Assert.IsTrue(complete);

            using( DfsInputStream stream = dfsClient.OpenFile(outputFileName) )
            using( StreamReader reader = new StreamReader(stream) )
            {
                Assert.AreEqual(_lines, Convert.ToInt32(reader.ReadLine()));
            }

            Console.WriteLine(config);
        }

        private static JobConfiguration CreateConfiguration(DfsClient dfsClient, string fileName, string outputFileName, bool forceFileDownload)
        {
            Tkl.Jumbo.Dfs.File file = dfsClient.NameServer.GetFileInfo(fileName);
            int blockSize = dfsClient.NameServer.BlockSize;

            JobConfiguration config = new JobConfiguration()
            {
                AssemblyFileName = "Tkl.Jumbo.Test.Tasks.dll",
                Tasks = new List<TaskConfiguration>(),
                Channels = new List<ChannelConfiguration>()
            };

            string[] tasks = new string[file.Blocks.Count];
            for( int x = 0; x < file.Blocks.Count; ++x )
            {
                config.Tasks.Add(new TaskConfiguration()
                {
                    TaskID = "Task" + (x + 1).ToString(),
                    TypeName = typeof(LineCounterTask).FullName,
                    DfsInput = new TaskDfsInput()
                    {
                        Path = fileName,
                        Block = x,
                        RecordReaderType = typeof(LineRecordReader).AssemblyQualifiedName
                    }
                });
                tasks[x] = "Task" + (x + 1).ToString();
            }

            config.Tasks.Add(new TaskConfiguration()
            {
                TaskID = "OutputTask",
                TypeName = typeof(LineAdderTask).FullName,
                DfsOutput = new TaskDfsOutput()
                {
                    Path = outputFileName,
                    RecordWriterType = typeof(TextRecordWriter<Int32Writable>).AssemblyQualifiedName
                }
            });

            config.Channels.Add(new ChannelConfiguration()
            {
                ChannelType = ChannelType.File,
                InputTasks = tasks,
                OutputTasks = new[] { "OutputTask" },
                ForceFileDownload = forceFileDownload
            });
            return config;
        }
    }
}
