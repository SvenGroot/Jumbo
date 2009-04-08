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

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class PipelineOutputChannelTests
    {
        private void TestCreateRecordWriterInternal<T>()
        {
            JobConfiguration config = CreateConfiguration();

            ChannelConfiguration channelConfig = config.Channels[0];
            //PipelineOutputChannel target = (PipelineOutputChannel)channelConfig.CreateOutputChannel(config, Utilities.TestOutputPath, "Task001");
        }

        private static JobConfiguration CreateConfiguration()
        {
            JobConfiguration config = new JobConfiguration(System.IO.Path.GetFileName(typeof(LineCounterTask).Assembly.Location));

            // We're just feeding it fake file information to create the job config, it doesn't matter since it'll never get executed.
            Directory dir = new Directory(null, "root", DateTime.UtcNow);
            File file = new File(dir, "myfile", DateTime.UtcNow);
            file.Blocks.Add(Guid.NewGuid());
            config.AddInputStage("Task", file, typeof(LineCounterTask), typeof(LineRecordReader));
            config.AddStage("OutputTask", new[] { "Task" }, typeof(LineAdderPushTask), 1, ChannelType.Pipeline, null, "/output", typeof(TextRecordWriter<Int32Writable>));

            return config;
        }
    }
}
