// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet;
using Tkl.Jumbo.Jet.Jobs.Builder;
using Tkl.Jumbo.Jet.Jobs;
using System.IO;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class DynamicTaskBuilderTests
    {
        [Test]
        public void TestCreateDynamicTask()
        {
            DynamicTaskBuilder target = new DynamicTaskBuilder();
            Action<RecordReader<int>, RecordWriter<int>, TaskContext> taskDelegate = TaskMethod;
            Type taskType = target.CreateDynamicTask(typeof(ITask<int, int>).GetMethod("Run"), taskDelegate, 0, RecordReuseMode.Allow);
            TaskContext context = CreateConfiguration(taskType);
            context.StageConfiguration.AddTypedSetting("Factor", 2);
            ITask<int, int> task = (ITask<int, int>)JetActivator.CreateInstance(taskType, null, null, context);
            List<int> data = Utilities.GenerateNumberData(10);
            List<int> result;
            using( EnumerableRecordReader<int> input = new EnumerableRecordReader<int>(data) )
            using( ListRecordWriter<int> output = new ListRecordWriter<int>() )
            {
                task.Run(input, output);
                result = output.List.ToList();
            }

            CollectionAssert.AreEqual(data.Select(i => i * 2).ToList(), result);
        }

        [Test]
        public void TestCreateDynamicTaskNoContext()
        {
            DynamicTaskBuilder target = new DynamicTaskBuilder();
            Action<RecordReader<int>, RecordWriter<int>> taskDelegate = TaskMethodNoContext;
            Type taskType = target.CreateDynamicTask(typeof(ITask<int, int>).GetMethod("Run"), taskDelegate, 0, RecordReuseMode.Allow);
            TaskContext context = CreateConfiguration(taskType);
            ITask<int, int> task = (ITask<int, int>)JetActivator.CreateInstance(taskType, null, null, context);
            List<int> data = Utilities.GenerateNumberData(10);
            List<int> result;
            using( EnumerableRecordReader<int> input = new EnumerableRecordReader<int>(data) )
            using( ListRecordWriter<int> output = new ListRecordWriter<int>() )
            {
                task.Run(input, output);
                result = output.List.ToList();
            }

            CollectionAssert.AreEqual(data.Select(i => i * 3).ToList(), result);
        }

        [Test]
        public void TestCreateDynamicTaskNonPublic()
        {
            DynamicTaskBuilder target = new DynamicTaskBuilder();
            Action<RecordReader<int>, RecordWriter<int>> taskDelegate = TaskMethodNonPublic;
            Type taskType = target.CreateDynamicTask(typeof(ITask<int, int>).GetMethod("Run"), taskDelegate, 0, RecordReuseMode.Allow);
            TaskContext context = CreateConfiguration(taskType);
            context.StageConfiguration.StageSettings = new SettingsDictionary();
            DynamicTaskBuilder.SerializeDelegate(context.StageConfiguration.StageSettings, taskDelegate);
            ITask<int, int> task = (ITask<int, int>)JetActivator.CreateInstance(taskType, null, null, context);
            List<int> data = Utilities.GenerateNumberData(10);
            List<int> result;
            using( EnumerableRecordReader<int> input = new EnumerableRecordReader<int>(data) )
            using( ListRecordWriter<int> output = new ListRecordWriter<int>() )
            {
                task.Run(input, output);
                result = output.List.ToList();
            }

            CollectionAssert.AreEqual(data.Select(i => i * 4).ToList(), result);
        }

        [Test]
        public void TestCreateDynamicTaskSkipParameters()
        {
            DynamicTaskBuilder target = new DynamicTaskBuilder();
            Action<RecordWriter<int>, TaskContext> taskDelegate = TaskMethodNoInput;
            Type taskType = target.CreateDynamicTask(typeof(ITask<int, int>).GetMethod("Run"), taskDelegate, 1, RecordReuseMode.Allow);
            TaskContext context = CreateConfiguration(taskType);
            context.StageConfiguration.AddTypedSetting("Count", 6);
            ITask<int, int> task = (ITask<int, int>)JetActivator.CreateInstance(taskType, null, null, context);
            List<int> result;
            using( ListRecordWriter<int> output = new ListRecordWriter<int>() )
            {
                task.Run(null, output);
                result = output.List.ToList();
            }

            CollectionAssert.AreEqual(Enumerable.Range(0, 6).ToList(), result);
        }
        
        public static void TaskMethod(RecordReader<int> input, RecordWriter<int> output, TaskContext context)
        {
            int factor = context.GetTypedSetting("Factor", 0);
            output.WriteRecords(input.EnumerateRecords().Select(i => i * factor));
        }

        public static void TaskMethodNoContext(RecordReader<int> input, RecordWriter<int> output)
        {
            int factor = 3;
            output.WriteRecords(input.EnumerateRecords().Select(i => i * factor));
        }

        public static void TaskMethodNoInput(RecordWriter<int> output, TaskContext context)
        {
            int count = context.GetTypedSetting("Count", 0);
            output.WriteRecords(Enumerable.Range(0, count));
        }
        
        private static void TaskMethodNonPublic(RecordReader<int> input, RecordWriter<int> output)
        {
            int factor = 4;
            output.WriteRecords(input.EnumerateRecords().Select(i => i * factor));
        }

        private TaskContext CreateConfiguration(Type taskType)
        {
            JobConfiguration job = new JobConfiguration();
            StageConfiguration stage = job.AddStage("TestStage", taskType, 1, null, null, null);
            return new TaskContext(Guid.Empty, job, new TaskAttemptId(new TaskId(stage.StageId, 1), 1), stage, Path.GetTempPath(), "/fake");
        }
    }
}
