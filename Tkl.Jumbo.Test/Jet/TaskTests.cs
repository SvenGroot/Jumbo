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
    }
}
