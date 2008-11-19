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
    public class JobServerTests
    {
        private TestJetCluster _cluster;

        [TestFixtureSetUp]
        public void Setup()
        {
            _cluster = new TestJetCluster(null, true, 2);
        }

        [TestFixtureTearDown]
        public void TearDown()
        {
            _cluster.Shutdown();
        }

        [Test]
        public void TestJobExecution()
        {
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            const int size = 150000000;
            const string fileName = "/jobinput.txt";
            const string outputFileName = "/joboutput.txt";
            int lines;
            using( DfsOutputStream stream = dfsClient.CreateFile("/jobinput.txt") )
            {
                lines = Utilities.GenerateDataLines(stream, size);
            }

            JobConfiguration config = CreateConfiguration(dfsClient, fileName, outputFileName);

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
                Assert.AreEqual(lines, Convert.ToInt32(reader.ReadLine()));
            }

            Console.WriteLine(config);
        }

        private static JobConfiguration CreateConfiguration(DfsClient dfsClient, string fileName, string outputFileName)
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
                OutputTaskID = "OutputTask"
            });
            return config;
        }
    }
}
