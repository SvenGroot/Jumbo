using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Jet.Tasks;
using Tkl.Jumbo.Jet.Channels;

namespace Tkl.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// Represents a sorting operation.
    /// </summary>
    public class SortOperation : TwoStepOperation
    {
        private readonly Type _comparerType;
        private readonly bool _useSpillSort;

        /// <summary>
        /// Initializes a new instance of the <see cref="SortOperation"/> class.
        /// </summary>
        /// <param name="builder">The job builder.</param>
        /// <param name="input">The input for this operation.</param>
        /// <param name="comparerType">The type of <see cref="IComparer{T}"/> to use for this operation, or <see langword="null"/> to use the default comparer. May be a generic type definition with a single type parameter.</param>
        /// <param name="useSpillSort">If set to <see langword="true"/> use a channel with output type <see cref="FileChannelOutputType.SortSpill"/> instead of a <see cref="SortTask{T}"/>.</param>
        public SortOperation(JobBuilder builder, IOperationInput input, Type comparerType, bool useSpillSort)
            : base(builder, input, typeof(SortTask<>), typeof(EmptyTask<>), true)
        {
            if( comparerType != null )
            {
                if( useSpillSort )
                    throw new NotSupportedException("Spill sorting doesn't support custom comparers.");

                if( comparerType.IsGenericTypeDefinition )
                    comparerType = comparerType.MakeGenericType(input.RecordType);
                if( comparerType.ContainsGenericParameters )
                    throw new ArgumentException("The comparer type must be a closed constructed generic type.", "comparerType");

                Type interfaceType = comparerType.FindGenericInterfaceType(typeof(IComparer<>));
                if( input.RecordType.IsSubclassOf(interfaceType.GetGenericArguments()[0]) )
                    throw new ArgumentException("The specified comparer cannot compare the record type.");
                builder.AddAssembly(comparerType.Assembly);
            }

            _comparerType = comparerType;
            InputChannel.MultiInputRecordReaderType = typeof(MergeRecordReader<>);
            StageId = "SortStage";
            SecondStepStageId = "MergeStage";
            _useSpillSort = useSpillSort;
        }

        /// <summary>
        /// Creates the configuration for this stage.
        /// </summary>
        /// <param name="compiler">The <see cref="JobBuilderCompiler"/>.</param>
        /// <returns>
        /// The <see cref="StageConfiguration"/> for the stage.
        /// </returns>
        protected override StageConfiguration CreateConfiguration(JobBuilderCompiler compiler)
        {
            if( _useSpillSort )
            {
                if( InputChannel.ChannelType == null )
                    InputChannel.ChannelType = ChannelType.File; // Spill sort requires file channel, so make sure it doesn't default to anything else

                InputStageInfo input = InputChannel.CreateInput();
                if( input.ChannelType != ChannelType.File )
                    throw new NotSupportedException("Spill sort can only be used on file channels.");

                input.InputStage.AddTypedSetting(FileOutputChannel.OutputTypeSettingKey, FileChannelOutputType.SortSpill);
                return compiler.CreateStage("MergeStage", SecondStepTaskType.TaskType, InputChannel.TaskCount, input, Output, true);
            }
            else
            {
                StageConfiguration result = base.CreateConfiguration(compiler);
                if( _comparerType != null )
                    FirstStepStage.AddSetting(TaskConstants.ComparerSettingKey, _comparerType.AssemblyQualifiedName);
                return result;
            }
        }
    }
}
