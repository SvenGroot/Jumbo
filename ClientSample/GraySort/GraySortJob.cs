using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.IO;

namespace ClientSample.GraySort
{
    static class GraySortJob
    {
        public static Guid RunJob(JetClient jetClient, DfsClient dfsClient, string inputFile, string outputPath)
        {
            dfsClient.NameServer.Delete(outputPath, true);
            dfsClient.NameServer.CreateDirectory(outputPath);

            JobConfiguration job = new JobConfiguration(typeof(GenSortRecordReader).Assembly);
            job.AddInputStage("SortStage", dfsClient.NameServer.GetFileInfo(inputFile), typeof(SortTask<GenSortRecord>), typeof(GenSortRecordReader));
            job.AddStage("MergeStage", new[] { "SortStage" }, typeof(MergeSortTask<GenSortRecord>), 1, Tkl.Jumbo.Jet.Channels.ChannelType.File, null, outputPath, typeof(GenSortRecordWriter));
            
            return jetClient.RunJob(job, typeof(GenSortRecordReader).Assembly.Location).JobID;
        }
    }
}
