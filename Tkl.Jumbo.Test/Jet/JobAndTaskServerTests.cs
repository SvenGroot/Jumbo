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
        public void TestJobExecutionSort()
        {
            const int recordCount = 2500000;
            const string inputFileName = "/sortinput";
            string outputPath = "/sortoutput";
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            dfsClient.NameServer.CreateDirectory(outputPath);

            List<int> expected = CreateNumberListInputFile(recordCount, inputFileName, dfsClient);
            expected.Sort();

            JobConfiguration config = new JobConfiguration(typeof(StringConversionTask).Assembly);
            StageConfiguration conversionStage = config.AddInputStage("ConversionStage", dfsClient.NameServer.GetFileInfo("/sortinput"), typeof(StringConversionTask), typeof(LineRecordReader));
            StageConfiguration sortStage = config.AddStage("SortStage", typeof(SortTask<Int32Writable>), 1, new InputStageInfo(conversionStage) { ChannelType = ChannelType.Pipeline }, null, null);
            config.AddStage("MergeStage", typeof(EmptyTask<Int32Writable>), 1, new InputStageInfo(sortStage) { MultiInputRecordReaderType = typeof(MergeRecordReader<Int32Writable>) }, outputPath, typeof(BinaryRecordWriter<Int32Writable>));

            RunJob(dfsClient, config);

            string outputFileName = DfsPath.Combine(outputPath, "MergeStage001");

            CheckOutput(dfsClient, expected, outputFileName);
        }

        [Test]
        public void TestJobSettings()
        {
            string outputPath = "/settingsoutput";
            DfsClient dfsClient = new DfsClient(Dfs.TestDfsCluster.CreateClientConfig());
            dfsClient.NameServer.CreateDirectory(outputPath);

            List<int> expected = CreateNumberListInputFile(10000, "/settingsinput", dfsClient);

            JobConfiguration config = new JobConfiguration(typeof(MultiplierTask).Assembly);
            config.AddInputStage("MultiplyStage", dfsClient.NameServer.GetFileInfo("/settingsinput"), typeof(MultiplierTask), typeof(LineRecordReader), outputPath, typeof(BinaryRecordWriter<Int32Writable>));
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
            StageConfiguration joinStage = config.AddStage("Join", typeof(EmptyTask<CustomerOrder>), joinTasks, new[] { customerSortInfo, orderSortInfo }, typeof(CustomerOrderJoinRecordReader), outputPath, typeof(RecordFileWriter<CustomerOrder>));

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
            var customerCollector = new RecordCollector<Customer>(null, null, joinTasks);
            var orderCollector = new RecordCollector<Order>(null, null, joinTasks);
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

        private static void CheckOutput(DfsClient dfsClient, IList<int> expected, string outputFileName)
        {
            List<int> actual = new List<int>();
            using( DfsInputStream stream = dfsClient.OpenFile(outputFileName) )
            using( BinaryRecordReader<Int32Writable> reader = new BinaryRecordReader<Int32Writable>(stream) )
            {
                while( reader.ReadRecord() )
                {
                    actual.Add(reader.CurrentRecord.Value);
                }
            }

            Assert.IsTrue(Utilities.CompareList(expected, actual));
        }

        private static void RunJob(DfsClient dfsClient, JobConfiguration config)
        {
            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, dfsClient, typeof(StringConversionTask).Assembly.Location);

            bool complete = target.JobServer.WaitForJobCompletion(job.JobId, Timeout.Infinite);
            Assert.IsTrue(complete);
        }

        private static List<int> CreateNumberListInputFile(int recordCount, string inputFileName, DfsClient dfsClient)
        {
            Random rnd = new Random();
            List<int> expected = new List<int>(recordCount);

            using( DfsOutputStream stream = dfsClient.CreateFile(inputFileName) )
            using( TextRecordWriter<Int32Writable> writer = new TextRecordWriter<Int32Writable>(stream) )
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
            }

            Tkl.Jumbo.Dfs.DfsFile file = dfsClient.NameServer.GetFileInfo(_fileName);
            JobConfiguration config = CreateConfiguration(dfsClient, file, outputPath, forceFileDownload, counterTask, adderTask, channelType);

            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, dfsClient, typeof(LineCounterTask).Assembly.Location);

            bool complete = target.JobServer.WaitForJobCompletion(job.JobId, Timeout.Infinite);
            Assert.IsTrue(complete);

            string outputFileName = DfsPath.Combine(outputPath, "OutputTask001");

            using( DfsInputStream stream = dfsClient.OpenFile(outputFileName) )
            using( StreamReader reader = new StreamReader(stream) )
            {
                Assert.AreEqual(_lines, Convert.ToInt32(reader.ReadLine()));
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
            config.AddStage("OutputTask", adderTask, 1, new InputStageInfo(stage) { ChannelType = channelType }, outputPath, typeof(TextRecordWriter<Int32Writable>));
            foreach( ChannelConfiguration channel in config.GetAllChannels() )
            {
                if( channel.ChannelType == ChannelType.File )
                    channel.ForceFileDownload = true;
            }

            return config;
        }
    }
}
