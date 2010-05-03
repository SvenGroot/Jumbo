// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;
using System.IO;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class TaskTests
    {
        #region Nested types

        private class TestAccumulator : AccumulatorTask<Utf8StringWritable, int>
        {
            protected override int Accumulate(Utf8StringWritable key, int currentValue, int newValue)
            {
                return currentValue + newValue;
            }
        }

        [AllowRecordReuse]
        private class TestRecordReuseAccumulator : AccumulatorTask<Utf8StringWritable, int>
        {
            protected override int Accumulate(Utf8StringWritable key, int currentValue, int newValue)
            {
                return currentValue + newValue;
            }
        }

        #endregion

        [TestFixtureSetUp]
        public void SetUp()
        {
            if( Directory.Exists(Utilities.TestOutputPath) )
                Directory.Delete(Utilities.TestOutputPath, true);
            Directory.CreateDirectory(Utilities.TestOutputPath);

            log4net.LogManager.ResetConfiguration();
            log4net.Config.BasicConfigurator.Configure();
        }

        [Test]
        public void TestSortTask()
        {
            const int recordCountMin = 1000;
            const int recordCountMax = 10000;
            Random rnd = new Random();
            int recordCount = rnd.Next(recordCountMin, recordCountMax);
            List<int> records = new List<int>(recordCount);
            for( int record = 0; record < recordCount; ++record )
            {
                int value = rnd.Next();
                records.Add(value);
            }
            ListRecordWriter<int> output = new ListRecordWriter<int>();

            SortTask<int> target = new SortTask<int>();
            foreach( int record in records )
                target.ProcessRecord(record, output);
            target.Finish(output);

            records.Sort();
            Assert.AreNotSame(records, output.List);
            Assert.IsTrue(Utilities.CompareList(records, output.List));
        }

        [Test]
        public void TestAccumulatorTask()
        {
            JobConfiguration jobConfig = new JobConfiguration();
            StageConfiguration stageConfig = jobConfig.AddStage("Accumulate", typeof(TestAccumulator), 1, null, null, null);
            TaskAttemptConfiguration config = new TaskAttemptConfiguration(Guid.NewGuid(), jobConfig, new TaskId("Accumulate", 1), stageConfig, Utilities.TestOutputPath, "/JumboJet/fake", 1, null);

            IPushTask<KeyValuePairWritable<Utf8StringWritable, int>, KeyValuePairWritable<Utf8StringWritable, int>> task = new TestAccumulator();
            JetActivator.ApplyConfiguration(task, null, null, config);
            ListRecordWriter<KeyValuePairWritable<Utf8StringWritable, int>> output = new ListRecordWriter<KeyValuePairWritable<Utf8StringWritable, int>>(true);

            task.ProcessRecord(new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("hello"), 1), output);
            task.ProcessRecord(new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("bye"), 2), output);
            task.ProcessRecord(new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("bye"), 3), output);
            task.ProcessRecord(new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("hello"), 4), output);
            task.ProcessRecord(new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("hello"), 5), output);
            task.ProcessRecord(new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("bye"), 1), output);
            task.ProcessRecord(new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("foo"), 1), output);
            task.ProcessRecord(new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("bye"), 1), output);

            task.Finish(output);

            var result = output.List;
            Assert.AreEqual(3, result.Count);
            Assert.Contains(new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("hello"), 10), result);
            Assert.Contains(new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("bye"), 7), result);
            Assert.Contains(new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("foo"), 1), result);
            Assert.Contains(new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("hello"), 10), result);
            CollectionAssert.DoesNotContain(result, new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("hello"), 9));
            CollectionAssert.DoesNotContain(result, new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("bar"), 1));
        }

        [Test]
        public void TestAccumulatorTaskRecordReuse()
        {
            JobConfiguration jobConfig = new JobConfiguration();
            StageConfiguration stageConfig = jobConfig.AddStage("Accumulate", typeof(TestAccumulator), 1, null, null, null);
            TaskAttemptConfiguration config = new TaskAttemptConfiguration(Guid.NewGuid(), jobConfig, new TaskId("Accumulate", 1), stageConfig, Utilities.TestOutputPath, "/JumboJet/fake", 1, null);

            IPushTask<KeyValuePairWritable<Utf8StringWritable, int>, KeyValuePairWritable<Utf8StringWritable, int>> task = new TestRecordReuseAccumulator();
            JetActivator.ApplyConfiguration(task, null, null, config);
            ListRecordWriter<KeyValuePairWritable<Utf8StringWritable, int>> output = new ListRecordWriter<KeyValuePairWritable<Utf8StringWritable, int>>(true);

            KeyValuePairWritable<Utf8StringWritable, int> record = new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("hello"), 1);
            task.ProcessRecord(record, output);
            record.Key.Set("bye");
            record.Value = 2;
            task.ProcessRecord(record, output);
            record.Key.Set("bye");
            record.Value = 3;
            task.ProcessRecord(record, output);
            record.Key.Set("hello");
            record.Value = 4;
            task.ProcessRecord(record, output);
            record.Key.Set("hello");
            record.Value = 5;
            task.ProcessRecord(record, output);
            record.Key.Set("bye");
            record.Value = 1;
            task.ProcessRecord(record, output);
            record.Key.Set("foo");
            record.Value = 1;
            task.ProcessRecord(record, output);
            record.Key.Set("bye");
            record.Value = 1;
            task.ProcessRecord(record, output);

            task.Finish(output);

            var result = output.List;
            Assert.AreEqual(3, result.Count);
            Assert.Contains(new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("hello"), 10), result);
            Assert.Contains(new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("bye"), 7), result);
            Assert.Contains(new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("foo"), 1), result);
            Assert.Contains(new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("hello"), 10), result);
            CollectionAssert.DoesNotContain(result, new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("hello"), 9));
            CollectionAssert.DoesNotContain(result, new KeyValuePairWritable<Utf8StringWritable, int>(new Utf8StringWritable("bar"), 1));
        }
    }
}
