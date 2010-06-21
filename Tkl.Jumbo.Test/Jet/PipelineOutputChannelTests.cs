// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Test.Tasks;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Dfs;
using System.Reflection;
using System.IO;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class PipelineOutputChannelTests
    {
        private const int _blockSize = 16 * 1024 * 1024;

        private class FakeUmbilical : ITaskServerUmbilicalProtocol
        {
            #region ITaskServerUmbilicalProtocol Members

            public void ReportCompletion(Guid jobID, TaskAttemptId taskID, TaskMetrics metrics)
            {
            }

            public void ReportProgress(Guid jobId, TaskAttemptId taskId, TaskProgress progress)
            {
            }

            public void SetUncompressedTemporaryFileSize(Guid jobId, string fileName, long uncompressedSize)
            {
            }

            public long GetUncompressedTemporaryFileSize(Guid jobId, string fileName)
            {
                return -1;
            }

            public void RegisterTcpChannelPort(Guid jobId, TaskAttemptId taskId, int port)
            {
            }

            #endregion
        }

        //[Test]
        //public void TestPipelineChannel()
        //{
        //    JobConfiguration config = CreateConfiguration();

        //    string path = System.IO.Path.Combine(Utilities.TestOutputPath, "PipelineChannelTest");
        //    if( System.IO.Directory.Exists(path) )
        //        System.IO.Directory.Delete(path, true);
        //    System.IO.Directory.CreateDirectory(path);
        //    // This depends on the fact that TaskExecutionUtility will not use the JetClient and DfsClient unless a DfsInput or DfsOutput is used. Since we will never
        //    // call GetInputReader that won't happen.
        //    using( TaskExecutionUtility taskExecution = new TaskExecutionUtility(new JetClient(), new FakeUmbilical(), Guid.NewGuid(), config, new TaskId("Task-001"), new DfsClient(), path, "/foo", 1) )
        //    {
        //        RecordWriter<int> output = taskExecution.GetOutputWriter<int>(); // this will call PipelineOutputChannel.CreateRecordWriter
        //        IPushTask<Utf8String, int> task = (IPushTask<Utf8String, int>)taskExecution.GetTaskInstance<Utf8String, int>();
        //        task.ProcessRecord(new Utf8String("Foo"), output);
        //        task.ProcessRecord(new Utf8String("Bar"), output);

        //        taskExecution.FinishTask();
        //    }
            
        //    // If this file contains the correct value is means that the first stage task wrote output to the pipeline channel, which invoked the second stage task which
        //    // wrote to the file, thus proving that pipelining works.
        //    using( System.IO.FileStream stream = System.IO.File.OpenRead(Path.Combine(Path.Combine(path, "Task-001"), "DummyTask.output")) )
        //    using( BinaryRecordReader<int> reader = new BinaryRecordReader<int>(stream) )
        //    {
        //        Assert.IsTrue(reader.ReadRecord());
        //        Assert.AreEqual(2, reader.CurrentRecord);
        //    }
        //}

        private static JobConfiguration CreateConfiguration()
        {
            JobConfiguration config = new JobConfiguration(System.IO.Path.GetFileName(typeof(LineCounterTask).Assembly.Location));

            // We're just feeding it fake file information to create the job config, it doesn't matter since we'll fake the input during the test.
            DfsDirectory dir = new DfsDirectory(null, "root", DateTime.UtcNow);
            DfsFile file = new DfsFile(dir, "myfile", DateTime.UtcNow, _blockSize, 1);
            file.Blocks.Add(Guid.NewGuid());
            StageConfiguration stage = config.AddInputStage("Task", file, typeof(LineCounterPushTask), typeof(LineRecordReader));
            stage = config.AddPointToPointStage("OutputTask", stage, typeof(LineAdderPushTask), ChannelType.Pipeline, null, null);

            // add a file channel with no outputs to collect the output in a dummy file.
            ChannelConfiguration channel = new ChannelConfiguration()
            {
                ChannelType = ChannelType.File,
                PartitionerType = typeof(HashPartitioner<int>),
            };
            stage.OutputChannel = channel;

            return config;
        }
    }
}
