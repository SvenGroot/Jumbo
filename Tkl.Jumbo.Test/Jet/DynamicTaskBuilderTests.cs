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
using System.Reflection;
using Tkl.Jumbo.Jet.Tasks;

namespace Tkl.Jumbo.Test.Jet
{
    [TestFixture]
    public class DynamicTaskBuilderTests
    {
        #region Nested types

        [Serializable]
        private class InstanceDelegateTest
        {
            public int Factor { get; set; }

            public void TaskMethod(RecordReader<int> input, RecordWriter<int> output)
            {
                output.WriteRecords(input.EnumerateRecords().Select(i => i * Factor));
            }
        }

        #endregion

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
        public void TestCreateDynamicTaskReturnType()
        {
            DynamicTaskBuilder target = new DynamicTaskBuilder();
            Func<Utf8String, int, int, int> taskDelegate = AccumulateMethod;
            Type taskType = target.CreateDynamicTask(typeof(AccumulatorTask<Utf8String, int>).GetMethod("Accumulate", BindingFlags.NonPublic | BindingFlags.Instance), taskDelegate, 0, RecordReuseMode.Allow);
            TaskContext context = CreateConfiguration(taskType);
            AccumulatorTask<Utf8String, int> task = (AccumulatorTask<Utf8String, int>)JetActivator.CreateInstance(taskType, null, null, context);
            List<string> data = Utilities.GenerateDataWords(null, 100, 10);
            List<KeyValuePair<string, int>> result;
            using( EnumerableRecordReader<Pair<Utf8String, int>> input = new EnumerableRecordReader<Pair<Utf8String, int>>(data.Select(w => Pair.MakePair(new Utf8String(w), 1)), data.Count) )
            using( ListRecordWriter<Pair<Utf8String, int>> output = new ListRecordWriter<Pair<Utf8String, int>>(true) )
            {
                task.Run(input, output);
                result = output.List.Select(p => new KeyValuePair<string, int>(p.Key.ToString(), p.Value)).OrderBy(p => p.Key, StringComparer.Ordinal).ToList();
            }

            List<KeyValuePair<string, int>> expected = (from word in data
                                                        group word by word into g
                                                        select new KeyValuePair<string, int>(g.Key, g.Count())).OrderBy(r => r.Key, StringComparer.Ordinal).ToList();

            CollectionAssert.AreEqual(expected, result);
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
        public void TestCreateDynamicTaskInstanceMethod()
        {
            DynamicTaskBuilder target = new DynamicTaskBuilder();
            InstanceDelegateTest instance = new InstanceDelegateTest() { Factor = 10 };
            Action<RecordReader<int>, RecordWriter<int>> taskDelegate = instance.TaskMethod;
            Type taskType = target.CreateDynamicTask(typeof(ITask<int, int>).GetMethod("Run"), taskDelegate, 0, RecordReuseMode.Allow);
            TaskContext context = CreateConfiguration(taskType);
            context.StageConfiguration.StageSettings = new SettingsDictionary();
            DynamicTaskBuilder.SerializeDelegate(context.StageConfiguration.StageSettings, taskDelegate);
            instance.Factor = 0; // Shouldn't affect the serialized instance.
            ITask<int, int> task = (ITask<int, int>)JetActivator.CreateInstance(taskType, null, null, context);
            List<int> data = Utilities.GenerateNumberData(10);
            List<int> result;
            using( EnumerableRecordReader<int> input = new EnumerableRecordReader<int>(data) )
            using( ListRecordWriter<int> output = new ListRecordWriter<int>() )
            {
                task.Run(input, output);
                result = output.List.ToList();
            }

            CollectionAssert.AreEqual(data.Select(i => i * 10).ToList(), result);
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

        [Test]
        public void TestCreateDynamicTaskAllowRecordReuse()
        {
            DynamicTaskBuilder target = new DynamicTaskBuilder();
            MethodInfo runMethod = typeof(ITask<int, int>).GetMethod("Run");
            // From attribute
            target = new DynamicTaskBuilder();
            Type type = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)TaskMethodAllowRecordReuse, 0, RecordReuseMode.Default);
            VerifyRecordReuse(type, true);
            // With passthrough
            target = new DynamicTaskBuilder();
            type = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)TaskMethodAllowRecordReusePassThrough, 0, RecordReuseMode.Default);
            VerifyRecordReuse(type, true, true);
            // Not allowed despite attribute
            target = new DynamicTaskBuilder();
            type = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)TaskMethodAllowRecordReuse, 0, RecordReuseMode.DontAllow);
            VerifyRecordReuse(type, false);
            // No attribute
            target = new DynamicTaskBuilder();
            type = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)TaskMethodNoContext, 0, RecordReuseMode.Default);
            VerifyRecordReuse(type, false);
            // No attribute with mode
            target = new DynamicTaskBuilder();
            type = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)TaskMethodNoContext, 0, RecordReuseMode.Allow);
            VerifyRecordReuse(type, true);
            // No attribute with mode (passthrough)
            target = new DynamicTaskBuilder();
            type = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)TaskMethodNoContext, 0, RecordReuseMode.PassThrough);
            VerifyRecordReuse(type, true, true);
            // Mode used for lambda
            target = new DynamicTaskBuilder();
            type = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)((input, output) => { }), 0, RecordReuseMode.Allow);
            VerifyRecordReuse(type, true);
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

        public static int AccumulateMethod(Utf8String key, int value, int newValue)
        {
            return value + newValue;
        }

        [AllowRecordReuse]
        public static void TaskMethodAllowRecordReuse(RecordReader<int> input, RecordWriter<int> output)
        {
        }

        [AllowRecordReuse(PassThrough=true)]
        public static void TaskMethodAllowRecordReusePassThrough(RecordReader<int> input, RecordWriter<int> output)
        {
        }

        private static void TaskMethodNonPublic(RecordReader<int> input, RecordWriter<int> output)
        {
            int factor = 4;
            output.WriteRecords(input.EnumerateRecords().Select(i => i * factor));
        }

        private static void VerifyRecordReuse(Type type, bool allow, bool passthrough = false)
        {
            AllowRecordReuseAttribute attribute = (AllowRecordReuseAttribute)Attribute.GetCustomAttribute(type, typeof(AllowRecordReuseAttribute));
            if( allow )
            {
                Assert.IsNotNull(attribute);
                Assert.AreEqual(passthrough, attribute.PassThrough);
            }
            else
                Assert.IsNull(attribute);
        }

        private TaskContext CreateConfiguration(Type taskType)
        {
            JobConfiguration job = new JobConfiguration();
            StageConfiguration stage = job.AddStage("TestStage", taskType, 1, null, null, null, null);
            return new TaskContext(Guid.Empty, job, new TaskAttemptId(new TaskId(stage.StageId, 1), 1), stage, Path.GetTempPath(), "/fake");
        }
    }
}
