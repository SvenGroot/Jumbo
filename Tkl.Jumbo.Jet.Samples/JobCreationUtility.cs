using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Jet.Samples
{
    static class JobCreationUtility
    {
        public static Guid RunTwoStageJob(DfsClient dfsClient, JetClient jetClient, Type inputTaskType, Type outputTaskType, string inputPath, string outputPath, int outputTaskCount)
        {
            CheckAndCreateOutputPath(dfsClient, outputPath);

            JobConfiguration config = new JobConfiguration(inputTaskType.Assembly);
            File file = dfsClient.NameServer.GetFileInfo(inputPath);
            if( file == null )
            {
                Directory dir = dfsClient.NameServer.GetDirectoryInfo(inputPath);
                if( dir == null )
                    throw new ArgumentException("The specified input path doesn't exist.", "inputPath");
                config.AddInputStage(inputTaskType.Name, dir, inputTaskType, typeof(LineRecordReader));
            }
            else
                config.AddInputStage(inputTaskType.Name, file, inputTaskType, typeof(LineRecordReader));
            Type interfaceType = outputTaskType.FindGenericInterfaceType(typeof(ITask<,>));
            Type outputType = interfaceType.GetGenericArguments()[1];
            config.AddStage(outputTaskType.Name, new[] { inputTaskType.Name }, outputTaskType, outputTaskCount, ChannelType.File, null, outputPath, typeof(TextRecordWriter<>).MakeGenericType(outputType));


            Job job = jetClient.RunJob(config, dfsClient, inputTaskType.Assembly.Location);

            return job.JobID;
        }

        private static void CheckAndCreateOutputPath(DfsClient dfsClient, string outputPath)
        {
            Directory outputDir = dfsClient.NameServer.GetDirectoryInfo(outputPath);
            if( outputDir != null )
                throw new ArgumentException("The specified output path already exists on the DFS.", "outputPath");
            dfsClient.NameServer.CreateDirectory(outputPath);
        }
    }
}
