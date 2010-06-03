﻿// $Id$
//
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
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.Jet.Jobs;

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
            NoOutput
        }

        private TestJetCluster _cluster;
        private const string _fileName = "/jobinput.txt";
        private int _lines;

        [TestFixtureSetUp]
        public void Setup()
        {
            _cluster = new TestJetCluster(16777216, true, 2, CompressionType.None, false);
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
        public void TestJobAbort()
        {
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            DfsFile file = dfsClient.NameServer.GetFileInfo(_fileName);
            JobConfiguration config = CreateConfiguration(dfsClient, file, "/abort", false, typeof(LineCounterTask), typeof(LineAdderTask), ChannelType.File);

            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, dfsClient, typeof(LineCounterTask).Assembly.Location);

            JobStatus status;
            do
            {
                Thread.Sleep(1000);
                status = target.JobServer.GetJobStatus(job.JobId);
            } while( status.RunningTaskCount == 0 );
            Thread.Sleep(1000);
            target.JobServer.AbortJob(job.JobId);
            bool finished = target.WaitForJobCompletion(job.JobId, Timeout.Infinite, 1000);
            Assert.IsTrue(finished);
            Assert.IsFalse(target.JobServer.GetJobStatus(job.JobId).IsSuccessful);
            Thread.Sleep(5000);
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
        public void TestJobExecutionTcpChannel()
        {
            RunJob(false, "/joboutput5", TaskKind.Pull, ChannelType.Tcp);
        }

        [Test]
        public void TestJobExecutionEmptyIntermediateData()
        {
            RunJob(false, "/joboutputempty", TaskKind.NoOutput, ChannelType.File);
        }

        [Test]
        public void TestJobExecutionEmptyIntermediateDataTcpFileDownload()
        {
            RunJob(true, "/joboutputempty2", TaskKind.NoOutput, ChannelType.File);
        }

        [Test]
        public void TestJobExecutionSort()
        {
            TestJobExecutionSort("/sortinput1", "/sortoutput1", 1, 1, false, false);
        }

        [Test]
        public void TestJobExecutionSortMultiplePartitionsPerTask()
        {
            TestJobExecutionSort("/sortinput2", "/sortoutput2", 2, 3, false, false);
        }

        [Test]
        public void TestJobExecutionSortMultiplePartitionsPerTaskTcpFileDownload()
        {
            TestJobExecutionSort("/sortinput3", "/sortoutput3", 2, 3, true, false);
        }

        [Test]
        public void TestJobExecutionSortSingleFileOutput()
        {
            TestJobExecutionSort("/sortinput4", "/sortoutput4", 2, 1, false, true);
        }

        [Test]
        public void TestJobExecutionSortSingleFileOutputTcpFileDownload()
        {
            TestJobExecutionSort("/sortinput5", "/sortoutput5", 2, 1, true, true);
        }

        [Test]
        public void TestJobSettings()
        {
            string outputPath = "/settingsoutput";
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            dfsClient.NameServer.CreateDirectory(outputPath);

            List<int> expected = CreateNumberListInputFile(10000, "/settingsinput", dfsClient);

            JobConfiguration config = new JobConfiguration(typeof(MultiplierTask).Assembly);
            config.AddInputStage("MultiplyStage", dfsClient.NameServer.GetFileInfo("/settingsinput"), typeof(MultiplierTask), typeof(LineRecordReader), outputPath, typeof(BinaryRecordWriter<int>));
            int factor = new Random().Next(2, 100);
            config.AddTypedSetting("factor", factor);

            RunJob(dfsClient, config);

            var multiplied = (from item in expected
                             select item * factor).ToList();
            CheckOutput(dfsClient, multiplied, DfsPath.Combine(outputPath, "MultiplyStage001"));
        }

        [Test]
        public void TestJobExecutionJoin()
        {
            List<Customer> customers = new List<Customer>();
            List<Order> orders = new List<Order>();

            Utilities.GenerateJoinData(customers, orders, 30000, 3, 100);
            customers.Randomize();
            orders.Randomize();

            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            dfsClient.NameServer.CreateDirectory("/testjoin");
            using( DfsOutputStream stream = dfsClient.CreateFile("/testjoin/customers") )
            using( RecordFileWriter<Customer> recordFile = new RecordFileWriter<Customer>(stream) )
            {
                foreach( Customer customer in customers )
                    recordFile.WriteRecord(customer);
            }

            using( DfsOutputStream stream = dfsClient.CreateFile("/testjoin/orders") )
            using( RecordFileWriter<Order> recordFile = new RecordFileWriter<Order>(stream) )
            {
                foreach( Order order in orders )
                    recordFile.WriteRecord(order);
            }
             
            const int joinTasks = 2;
            JobConfiguration config = new JobConfiguration(typeof(CustomerOrderJoinRecordReader).Assembly);
            StageConfiguration customerInput = config.AddInputStage("CustomerInput", dfsClient.NameServer.GetFileInfo("/testjoin/customers"), typeof(EmptyTask<Customer>), typeof(RecordFileReader<Customer>));
            StageConfiguration customerSort = config.AddStage("CustomerSort", typeof(SortTask<Customer>), joinTasks, new InputStageInfo(customerInput) { ChannelType = ChannelType.Pipeline }, null, null);
            StageConfiguration orderInput = config.AddInputStage("OrderInput", dfsClient.NameServer.GetFileInfo("/testjoin/orders"), typeof(EmptyTask<Order>), typeof(RecordFileReader<Order>));
            StageConfiguration orderSort = config.AddStage("OrderSort", typeof(SortTask<Order>), joinTasks, new InputStageInfo(orderInput) { ChannelType = ChannelType.Pipeline }, null, null);

            orderInput.AddSetting(PartitionerConstants.EqualityComparerSetting, typeof(OrderJoinComparer).AssemblyQualifiedName);
            orderSort.AddSetting(SortTaskConstants.ComparerSetting, typeof(OrderJoinComparer).AssemblyQualifiedName);
            orderSort.AddSetting(MergeRecordReaderConstants.ComparerSetting, typeof(OrderJoinComparer).AssemblyQualifiedName);

            const string outputPath = "/testjoinoutput";
            dfsClient.NameServer.CreateDirectory(outputPath);
            InputStageInfo customerSortInfo = new InputStageInfo(customerSort)
            {
                MultiInputRecordReaderType = typeof(MergeRecordReader<Customer>)
            };
            InputStageInfo orderSortInfo = new InputStageInfo(orderSort)
            {
                MultiInputRecordReaderType = typeof(MergeRecordReader<Order>)
            };
            config.AddStage("Join", typeof(EmptyTask<CustomerOrder>), joinTasks, new[] { customerSortInfo, orderSortInfo }, typeof(CustomerOrderJoinRecordReader), outputPath, typeof(RecordFileWriter<CustomerOrder>));

            RunJob(dfsClient, config);

            List<CustomerOrder> actual = new List<CustomerOrder>();
            for( int x = 0; x < joinTasks; ++x )
            {
                using( DfsInputStream stream = dfsClient.OpenFile(DfsPath.Combine(outputPath, string.Format("Join{0:000}", x+1))) )
                using( RecordFileReader<CustomerOrder> reader = new RecordFileReader<CustomerOrder>(stream) )
                {
                    while( reader.ReadRecord() )
                    {
                        actual.Add(reader.CurrentRecord);
                    }
                }
            }

            List<CustomerOrder> expected = (from customer in customers
                                            join order in orders on customer.Id equals order.CustomerId
                                            select new CustomerOrder() { CustomerId = customer.Id, ItemId = order.ItemId, Name = customer.Name, OrderId = order.Id }).ToList();
            expected.Sort();
            actual.Sort();

            Assert.IsTrue(Utilities.CompareList(expected, actual));
        }

        [Test]
        public void TestJobExecutionJobBuilderJoin()
        {
            List<Customer> customers = new List<Customer>();
            List<Order> orders = new List<Order>();

            Utilities.GenerateJoinData(customers, orders, 30000, 3, 100);
            customers.Randomize();
            orders.Randomize();

            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            dfsClient.NameServer.CreateDirectory("/testjbjoin");
            using( DfsOutputStream stream = dfsClient.CreateFile("/testjbjoin/customers") )
            using( RecordFileWriter<Customer> recordFile = new RecordFileWriter<Customer>(stream) )
            {
                foreach( Customer customer in customers )
                    recordFile.WriteRecord(customer);
            }

            using( DfsOutputStream stream = dfsClient.CreateFile("/testjbjoin/orders") )
            using( RecordFileWriter<Order> recordFile = new RecordFileWriter<Order>(stream) )
            {
                foreach( Order order in orders )
                    recordFile.WriteRecord(order);
            }

            const string outputPath = "/testjbjoinoutput";
            const int joinTasks = 2;

            JobBuilder builder = new JobBuilder(dfsClient, new JetClient(TestJetCluster.CreateClientConfig()));

            var customerInput = builder.CreateRecordReader<Customer>("/testjbjoin/customers", typeof(RecordFileReader<Customer>));
            var orderInput = builder.CreateRecordReader<Order>("/testjbjoin/orders", typeof(RecordFileReader<Order>));
            var customerCollector = new RecordCollector<Customer>() { PartitionCount = joinTasks };
            var orderCollector = new RecordCollector<Order>() { PartitionCount = joinTasks };
            var output = builder.CreateRecordWriter<CustomerOrder>(outputPath, typeof(RecordFileWriter<CustomerOrder>));

            builder.PartitionRecords(customerInput, customerCollector.CreateRecordWriter(), "CustomerInputStage");
            builder.PartitionRecords(orderInput, orderCollector.CreateRecordWriter(), "OrderInputStage");
            builder.JoinRecords(customerCollector.CreateRecordReader(), orderCollector.CreateRecordReader(), output, typeof(CustomerOrderJoinRecordReader), null, typeof(OrderJoinComparer));

            dfsClient.NameServer.CreateDirectory(outputPath);

            RunJob(dfsClient, builder.JobConfiguration);

            List<CustomerOrder> actual = new List<CustomerOrder>();
            for( int x = 0; x < joinTasks; ++x )
            {
                using( DfsInputStream stream = dfsClient.OpenFile(DfsPath.Combine(outputPath, string.Format("JoinStage{0:000}", x + 1))) )
                using( RecordFileReader<CustomerOrder> reader = new RecordFileReader<CustomerOrder>(stream) )
                {
                    while( reader.ReadRecord() )
                    {
                        actual.Add(reader.CurrentRecord);
                    }
                }
            }

            List<CustomerOrder> expected = (from customer in customers
                                            join order in orders on customer.Id equals order.CustomerId
                                            select new CustomerOrder() { CustomerId = customer.Id, ItemId = order.ItemId, Name = customer.Name, OrderId = order.Id }).ToList();
            expected.Sort();
            actual.Sort();

            Assert.IsTrue(Utilities.CompareList(expected, actual));

        }

        private void TestJobExecutionSort(string inputFileName, string outputPath, int mergeTasks, int partitionsPerTask, bool forceFileDownload, bool singleFileOutput)
        {
            const int recordCount = 2500000;
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            dfsClient.NameServer.CreateDirectory(outputPath);

            List<int> expected = CreateNumberListInputFile(recordCount, inputFileName, dfsClient);
            expected.Sort();

            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", dfsClient.NameServer.GetFileInfo(inputFileName), typeof(StringConversionTask), typeof(LineRecordReader));
            int taskCount = singleFileOutput ? 1 : mergeTasks * partitionsPerTask; // single file output does not currently support internal partitioning.
            StageConfiguration sortStage = config.AddStage("SortStage", typeof(SortTask<int>), taskCount, new InputStageInfo(conversionStage) { ChannelType = ChannelType.Pipeline }, null, null);
            if( singleFileOutput )
            {
                sortStage.AddTypedSetting(FileOutputChannel.SingleFileOutputSettingKey, true);
                sortStage.AddTypedSetting(FileOutputChannel.SingleFileOutputBufferSizeSettingKey, "1MB");
            }
            config.AddStage("MergeStage", typeof(EmptyTask<int>), mergeTasks, new InputStageInfo(sortStage) { MultiInputRecordReaderType = typeof(MergeRecordReader<int>), PartitionsPerTask = partitionsPerTask }, outputPath, typeof(BinaryRecordWriter<int>));
            sortStage.OutputChannel.ForceFileDownload = forceFileDownload;

            RunJob(dfsClient, config);

            CheckOutput(dfsClient, expected, outputPath);
        }

        private static void CheckOutput(DfsClient dfsClient, IList<int> expected, string outputPath)
        {
            List<int> actual = new List<int>();
            IList<int>[] partitions;

            IEnumerable<string> fileNames;
            FileSystemEntry entry = dfsClient.NameServer.GetFileSystemEntryInfo(outputPath);
            if( entry is DfsFile )
            {
                fileNames = new[] { entry.FullPath };
            }
            else
            {
                fileNames = from child in ((DfsDirectory)entry).Children
                            where child is DfsFile
                            orderby child.FullPath
                            select child.FullPath;

                // We need to create partitions that match the output.
            }

            int partitionCount = fileNames.Count();
            if( partitionCount == 1 )
                partitions = new[] { expected };
            else
            {
                HashPartitioner<int> partitioner = new HashPartitioner<int>();
                partitioner.Partitions = partitionCount;
                partitions = new IList<int>[partitionCount];
                for( int x = 0; x < partitionCount; ++x )
                {
                    partitions[x] = new List<int>();
                }

                foreach( int x in expected )
                    partitions[partitioner.GetPartition(x)].Add(x);
            }

            int p = 0;
            foreach( IList<int> part in partitions )
            {
                using( StreamWriter writer = File.CreateText(Path.Combine(Utilities.TestOutputPath, string.Format("partition{0}.txt", p))) )
                {
                    foreach( int item in part )
                        writer.WriteLine(item);
                }
                ++p;
            }

            int partition = 0;
            foreach( string outputFileName in fileNames )
            {
                actual.Clear();
                using( DfsInputStream stream = dfsClient.OpenFile(outputFileName) )
                using( BinaryRecordReader<int> reader = new BinaryRecordReader<int>(stream) )
                {
                    while( reader.ReadRecord() )
                    {
                        actual.Add(reader.CurrentRecord);
                    }
                }
                CollectionAssert.AreEqual(partitions[partition], actual);
                ++partition;
            }

        }

        private static void RunJob(DfsClient dfsClient, JobConfiguration config)
        {
            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, dfsClient, typeof(StringConversionTask).Assembly.Location);

            bool complete = target.WaitForJobCompletion(job.JobId, Timeout.Infinite, 1000);
            Assert.IsTrue(complete);
        }

        private static List<int> CreateNumberListInputFile(int recordCount, string inputFileName, DfsClient dfsClient)
        {
            Random rnd = new Random();
            List<int> expected = new List<int>(recordCount);

            using( DfsOutputStream stream = dfsClient.CreateFile(inputFileName) )
            using( TextRecordWriter<int> writer = new TextRecordWriter<int>(stream) )
            {
                for( int x = 0; x < recordCount; ++x )
                {
                    int record = rnd.Next();
                    expected.Add(record);
                    writer.WriteRecord(record);
                }
            }
            return expected;
        }

        private void RunJob(bool forceFileDownload, string outputPath, TaskKind taskKind, ChannelType channelType)
        {
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            dfsClient.NameServer.CreateDirectory(outputPath);

            int lines = _lines;
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
            case TaskKind.NoOutput:
                counterTask = typeof(NoOutputTask);
                adderTask = typeof(LineAdderTask);
                lines = 0;
                break;
            }

            Tkl.Jumbo.Dfs.DfsFile file = dfsClient.NameServer.GetFileInfo(_fileName);
            JobConfiguration config = CreateConfiguration(dfsClient, file, outputPath, forceFileDownload, counterTask, adderTask, channelType);

            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, dfsClient, typeof(LineCounterTask).Assembly.Location);

            bool complete = target.WaitForJobCompletion(job.JobId, Timeout.Infinite, 1000);
            Assert.IsTrue(complete);

            string outputFileName = DfsPath.Combine(outputPath, "OutputTask001");

            using( DfsInputStream stream = dfsClient.OpenFile(outputFileName) )
            using( StreamReader reader = new StreamReader(stream) )
            {
                Assert.AreEqual(lines, Convert.ToInt32(reader.ReadLine()));
            }

            Console.WriteLine(config);
        }

        private static JobConfiguration CreateConfiguration(DfsClient dfsClient, Tkl.Jumbo.Dfs.DfsFile file, string outputPath, bool forceFileDownload, Type counterTask, Type adderTask, ChannelType channelType)
        {

            JobConfiguration config = new JobConfiguration(System.IO.Path.GetFileName(typeof(LineCounterTask).Assembly.Location));

            StageConfiguration stage = config.AddInputStage("Task", file, counterTask, typeof(LineRecordReader));
            if( channelType == ChannelType.Pipeline )
            {
                // Pipeline channel cannot merge so we will add another stage in between.
                stage = config.AddPointToPointStage("IntermediateTask", stage, adderTask, ChannelType.Pipeline, null, null);
                channelType = ChannelType.File;
            }
            config.AddStage("OutputTask", adderTask, 1, new InputStageInfo(stage) { ChannelType = channelType }, outputPath, typeof(TextRecordWriter<int>));
            if( forceFileDownload )
                config.AddTypedSetting(FileInputChannel.MemoryStorageSizeSetting, 0L);
            foreach( ChannelConfiguration channel in config.GetAllChannels() )
            {
                if( channel.ChannelType == ChannelType.File )
                    channel.ForceFileDownload = forceFileDownload;
            }

            return config;
        }
    }
}
