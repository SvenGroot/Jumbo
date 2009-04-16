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
        public static Guid RunGraySortJob(JetClient jetClient, DfsClient dfsClient, string inputFile, string outputPath, int mergeTasks)
        {
            dfsClient.NameServer.Delete(outputPath, true);
            dfsClient.NameServer.CreateDirectory(outputPath);

            JobConfiguration job = new JobConfiguration(typeof(GenSortRecordReader).Assembly);
            job.AddInputStage("SortStage", dfsClient.NameServer.GetFileInfo(inputFile), typeof(SortTask<GenSortRecord>), typeof(GenSortRecordReader));
            job.AddStage("MergeStage", new[] { "SortStage" }, typeof(MergeSortTask<GenSortRecord>), mergeTasks, Tkl.Jumbo.Jet.Channels.ChannelType.File, typeof(RangePartitioner), outputPath, typeof(GenSortRecordWriter));

            if( mergeTasks > 1 )
            {
                const string partitionFile = "/graysortpartitions";
                RangePartitioner.CreatePartitionFile(dfsClient, partitionFile, (from task in job.Tasks where task.DfsInput != null select task.DfsInput).ToArray(), mergeTasks, 10000);
                job.JobSettings = new SettingsDictionary();
                job.JobSettings["partitionFile"] = partitionFile;
            }
            
            return jetClient.RunJob(job, typeof(GenSortRecordReader).Assembly.Location).JobID;
        }

        public static Guid RunGenSortJob(JetClient jetClient, DfsClient dfsClient, ulong startRecord, ulong count, int tasks, string outputPath)
        {
            dfsClient.NameServer.Delete(outputPath, true);
            dfsClient.NameServer.CreateDirectory(outputPath);

            JobConfiguration job = new JobConfiguration(typeof(GenSortTask).Assembly);

            ulong countPerTask = count / (ulong)tasks;

            for( uint x = 0; x < tasks; ++x )
            {
                TaskConfiguration task = new TaskConfiguration()
                {
                    TaskID = "GenSort" + (x + 1).ToString("000"),
                    TaskType = typeof(GenSortTask),
                    TaskSettings = new SettingsDictionary(),
                    DfsOutput = new TaskDfsOutput()
                    {
                        Path = DfsPath.Combine(outputPath, "GenSort" + (x + 1).ToString("000")),
                        RecordWriterType = typeof(BinaryRecordWriter<ByteArrayWritable>).AssemblyQualifiedName
                    }
                };
                task.TaskSettings["startRecord"] = (startRecord + x * countPerTask).ToString();
                task.TaskSettings["count"] = countPerTask.ToString();
                job.Tasks.Add(task);
            }

            return jetClient.RunJob(job, typeof(GenSortTask).Assembly.Location).JobID;
        }
    }
}
