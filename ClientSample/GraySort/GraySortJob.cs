using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo;

namespace ClientSample.GraySort
{
    static class GraySortJob
    {
        public static Guid RunGraySortJob(JetClient jetClient, DfsClient dfsClient, string inputFile, string outputPath, int mergeTasks, int maxMergeInputs, bool useCompression)
        {
            dfsClient.NameServer.Delete(outputPath, true);
            dfsClient.NameServer.CreateDirectory(outputPath);

            JobConfiguration job = new JobConfiguration(typeof(GenSortRecordReader).Assembly);
            File file = dfsClient.NameServer.GetFileInfo(inputFile);
            Directory dir = null;
            if( file == null )
                dir = dfsClient.NameServer.GetDirectoryInfo(inputFile);
            if( dir == null )
                job.AddInputStage("InputStage", dfsClient.NameServer.GetFileInfo(inputFile), typeof(EmptyTask<GenSortRecord>), typeof(GenSortRecordReader));
            else
                job.AddInputStage("InputStage", dir, typeof(EmptyTask<GenSortRecord>), typeof(GenSortRecordReader));
            job.AddPointToPointStage("SortStage", "InputStage", typeof(SortTask<GenSortRecord>), Tkl.Jumbo.Jet.Channels.ChannelType.Pipeline, typeof(RangePartitioner), null, null);

            job.AddStage("MergeStage", new[] { "SortStage" }, typeof(MergeSortTask<GenSortRecord>), 1, Tkl.Jumbo.Jet.Channels.ChannelType.File, typeof(RangePartitioner), outputPath, typeof(GenSortRecordWriter));
            if( maxMergeInputs > 0 )
            {
                Console.WriteLine("Using {0} max merge inputs per pass.", maxMergeInputs);
                job.AddTypedSetting(MergeSortTask<GenSortRecord>.MaxMergeInputsSetting, maxMergeInputs);
            }

            if( mergeTasks > 1 )
            {
                job.SplitStageOutput(new[] { "InputStage" }, mergeTasks);
                const string partitionFile = "/graysortpartitions";
                RangePartitioner.CreatePartitionFile(dfsClient, partitionFile, (from task in job.Tasks where task.DfsInput != null select task.DfsInput).ToArray(), mergeTasks, 10000);
                job.AddSetting("partitionFile", partitionFile);
            }

            if( useCompression )
            {
                Console.WriteLine("Enabling compression.");
                job.AddTypedSetting(FileOutputChannel.CompressionTypeSetting, CompressionType.GZip);
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

        public static Guid RunValSortJob(JetClient jetClient, DfsClient dfsClient, string inputPath, string outputPath)
        {
            dfsClient.NameServer.Delete(outputPath, true);
            dfsClient.NameServer.CreateDirectory(outputPath);
            JobConfiguration job = new JobConfiguration(typeof(ValSortTask).Assembly);

            File file = dfsClient.NameServer.GetFileInfo(inputPath);
            if( file == null )
                job.AddInputStage("ValSortStage", dfsClient.NameServer.GetDirectoryInfo(inputPath), typeof(ValSortTask), typeof(GenSortRecordReader));
            else
                job.AddInputStage("ValSortStage", file, typeof(ValSortTask), typeof(GenSortRecordReader));

            // Sort the records by input task name, this ensures that the combiner task gets the records in order of file so it can easily compre the first and last records
            // of consecutive files.
            job.AddStage("SortStage", new[] { "ValSortStage" }, typeof(SortTask<ValSortRecord>), 1, Tkl.Jumbo.Jet.Channels.ChannelType.File, null, null, null);
            job.AddPointToPointStage("CombinerStage", "SortStage", typeof(ValSortCombinerTask), Tkl.Jumbo.Jet.Channels.ChannelType.Pipeline, null, outputPath, typeof(TextRecordWriter<StringWritable>));

            return jetClient.RunJob(job, typeof(ValSortTask).Assembly.Location).JobID;
        }
    }
}
