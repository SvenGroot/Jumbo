﻿// $Id$
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

        private class TestAccumulator : AccumulatorTask<Utf8StringWritable, Int32Writable>
        {
            protected override void Accumulate(Utf8StringWritable key, Int32Writable value, Int32Writable newValue)
            {
                value.Value += newValue.Value;
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
            List<Int32Writable> records = new List<Int32Writable>(recordCount);
            for( int record = 0; record < recordCount; ++record )
            {
                int value = rnd.Next();
                records.Add(value);
            }
            ListRecordWriter<Int32Writable> output = new ListRecordWriter<Int32Writable>();

            SortTask<Int32Writable> target = new SortTask<Int32Writable>();
            foreach( Int32Writable record in records )
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

            IPushTask<KeyValuePairWritable<Utf8StringWritable, Int32Writable>, KeyValuePairWritable<Utf8StringWritable, Int32Writable>> task = new TestAccumulator();
            JetActivator.ApplyConfiguration(task, null, null, config);
            ListRecordWriter<KeyValuePairWritable<Utf8StringWritable, Int32Writable>> output = new ListRecordWriter<KeyValuePairWritable<Utf8StringWritable, Int32Writable>>(true);

            task.ProcessRecord(new KeyValuePairWritable<Utf8StringWritable, Int32Writable>(new Utf8StringWritable("hello"), 1), output);
            task.ProcessRecord(new KeyValuePairWritable<Utf8StringWritable, Int32Writable>(new Utf8StringWritable("bye"), 2), output);
            task.ProcessRecord(new KeyValuePairWritable<Utf8StringWritable, Int32Writable>(new Utf8StringWritable("bye"), 3), output);
            task.ProcessRecord(new KeyValuePairWritable<Utf8StringWritable, Int32Writable>(new Utf8StringWritable("hello"), 4), output);
            task.ProcessRecord(new KeyValuePairWritable<Utf8StringWritable, Int32Writable>(new Utf8StringWritable("hello"), 5), output);
            task.ProcessRecord(new KeyValuePairWritable<Utf8StringWritable, Int32Writable>(new Utf8StringWritable("bye"), 1), output);
            task.ProcessRecord(new KeyValuePairWritable<Utf8StringWritable, Int32Writable>(new Utf8StringWritable("foo"), 1), output);
            task.ProcessRecord(new KeyValuePairWritable<Utf8StringWritable, Int32Writable>(new Utf8StringWritable("bye"), 1), output);

            task.Finish(output);

            var result = output.List;
            Assert.AreEqual(3, result.Count);
            Assert.Contains(new KeyValuePairWritable<Utf8StringWritable, Int32Writable>(new Utf8StringWritable("hello"), 10), result);
            Assert.Contains(new KeyValuePairWritable<Utf8StringWritable, Int32Writable>(new Utf8StringWritable("bye"), 7), result);
            Assert.Contains(new KeyValuePairWritable<Utf8StringWritable, Int32Writable>(new Utf8StringWritable("foo"), 1), result);
            Assert.Contains(new KeyValuePairWritable<Utf8StringWritable, Int32Writable>(new Utf8StringWritable("hello"), 10), result);
            CollectionAssert.DoesNotContain(result, new KeyValuePairWritable<Utf8StringWritable, Int32Writable>(new Utf8StringWritable("hello"), 9));
            CollectionAssert.DoesNotContain(result, new KeyValuePairWritable<Utf8StringWritable, Int32Writable>(new Utf8StringWritable("bar"), 1));
        }
    }
}
