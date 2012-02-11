// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Dfs.FileSystem;
using System.IO;
using Tkl.Jumbo.Jet.Jobs.Builder;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Test.Tasks;
using Tkl.Jumbo.Jet;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class JobAndTaskServerLocalFileSystemTests
    {
        private TestJetCluster _cluster;
        private const string _inputPath = "/input";
        private const int _maxTasks = 2;
        private int _lines;

        [TestFixtureSetUp]
        public void Setup()
        {
            _cluster = new TestJetCluster(16777216, true, _maxTasks, CompressionType.None, true);
            FileSystemClient fileSystemClient = _cluster.CreateFileSystemClient();
            fileSystemClient.CreateDirectory(_inputPath);
            const int size = 500000;
            for( int x = 0; x < 3; ++x )
            {
                using( Stream stream = fileSystemClient.CreateFile(fileSystemClient.Path.Combine(_inputPath, "file" + x)) )
                {
                    _lines += Utilities.GenerateDataLines(stream, size);
                }
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
            JetClient jetClient = TestJetCluster.CreateJetClient();
            FileSystemClient fsClient = _cluster.CreateFileSystemClient();
            Assert.IsInstanceOf<LocalFileSystemClient>(fsClient); // Make sure we're local
            JobBuilder builder = new JobBuilder(fsClient, jetClient);
            var input = builder.Read(_inputPath, typeof(LineRecordReader));
            var counted = builder.Process(input, typeof(LineCounterTask));
            var added = builder.Process(counted, typeof(LineAdderTask));
            added.StageId = "OutputTask"; // ValidateLineCountOutput requires that for the output file name
            added.InputChannel.PartitionCount = 1;
            builder.Write(added, "/output", typeof(TextRecordWriter<>));

            fsClient.CreateDirectory("/output");

            JobAndTaskServerTests.RunJob(fsClient, builder.CreateJob());
            JobAndTaskServerTests.ValidateLineCountOutput("/output", fsClient, _lines);
        }
    }
}
