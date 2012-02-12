// $Id$
//
using System;
using System.Collections.Generic;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Provides information about an input stage to the <see cref="JobConfiguration.AddStage(string, Type, int, InputStageInfo)"/> method.
    /// </summary>
    public class InputStageInfo
    {
        private Type _partitionerType;
        private Type _multiInputRecordReaderType;

        /// <summary>
        /// Initializes a new instance of the <see cref="InputStageInfo"/> class.
        /// </summary>
        /// <param name="inputStage">The stage configuration of the input stage.</param>
        public InputStageInfo(StageConfiguration inputStage)
        {
            if( inputStage == null )
                throw new ArgumentNullException("inputStage");

            InputStage = inputStage;
            PartitionsPerTask = 1;
        }

        /// <summary>
        /// Gets the stage configuration of the input stage.
        /// </summary>
        public StageConfiguration InputStage { get; private set; }

        /// <summary>
        /// Gets the type of the channel to use.
        /// </summary>
        public ChannelType ChannelType { get; set; }

        /// <summary>
        /// Gets the type of channel connectivity to use.
        /// </summary>
        public ChannelConnectivity ChannelConnectivity { get; set; }

        /// <summary>
        /// Gets the type of partitioner to use.
        /// </summary>
        public Type PartitionerType
        {
            get 
            { 
                return _partitionerType ?? typeof(HashPartitioner<>).MakeGenericType(InputStageOutputType); 
            }
            set { _partitionerType = value; }
        }

        /// <summary>
        /// Gets the number of partitions to create for each output task.
        /// </summary>
        public int PartitionsPerTask { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to disable dynamic partition assignment.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if dynamic partition assignment; otherwise, <see langword="false"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   If <see cref="PartitionsPerTask"/> is larger than 1, tasks can dynamically receive additional partitions
        ///   besides those that were initially assigned to them.
        /// </para>
        /// <para>
        ///   If this property is set to <see langword="true"/>, dynamic partition assignment is disabled for
        ///   the receiving stage of this channel, and every task will only ever process the partitions it was
        ///   initially assigned.
        /// </para>
        /// </remarks>
        public bool DisableDynamicPartitionAssignment { get; set; }

        /// <summary>
        /// Gets or sets the method used to assign partitions to tasks when the job is started.
        /// </summary>
        /// <value>The partition assignment method.</value>
        public PartitionAssignmentMethod PartitionAssignmentMethod { get; set; }

        /// <summary>
        /// Gets the type of multi input record reader to use.
        /// </summary>
        public Type MultiInputRecordReaderType
        {
            get
            {
                return _multiInputRecordReaderType ?? (ChannelType == ChannelType.Tcp ? typeof(RoundRobinMultiInputRecordReader<>).MakeGenericType(InputStageOutputType) : typeof(MultiRecordReader<>).MakeGenericType(InputStageOutputType));
            }
            set { _multiInputRecordReaderType = value; }
        }

        private Type InputStageOutputType
        {
            get { return InputStage.TaskType.ReferencedType.FindGenericInterfaceType(typeof(ITask<,>), true).GetGenericArguments()[1]; }
        }

        private void ValidatePartitionerType()
        {
            // Get the output type of the input stage, which is the input to the partitioner.
            Type inputType = InputStageOutputType;
            Type partitionerInterfaceType = PartitionerType.FindGenericInterfaceType(typeof(IPartitioner<>));
            Type partitionedType = partitionerInterfaceType.GetGenericArguments()[0];
            if( partitionedType != inputType )
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The partitioner type {0} cannot partition objects of type {1}.", PartitionerType, inputType));
        }

        internal void ValidateTypes(Type stageMultiInputRecordReaderType, Type inputType)
        {
            ValidatePartitionerType();
            ValidateMultiInputRecordReaderType(stageMultiInputRecordReaderType, inputType);
        }

        private void ValidateMultiInputRecordReaderType(Type stageMultiInputRecordReaderType, Type inputType)
        {
            List<Type> acceptedInputTypes = new List<Type>();
            Type baseType;
            Type recordType;
            if( stageMultiInputRecordReaderType != null )
            {
                // The output of the stage multi input record reader type must match the input type of the stage.
                baseType = stageMultiInputRecordReaderType.FindGenericBaseType(typeof(MultiInputRecordReader<>), true);
                recordType = baseType.GetGenericArguments()[0];
                if( recordType != inputType )
                    throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The specified stage multi input record reader type {0} doesn't return objects of type {1}.", stageMultiInputRecordReaderType, inputType), "stageMultiInputRecordReaderType");

                acceptedInputTypes = GetMultiInputRecordReaderAcceptedInputTypes(stageMultiInputRecordReaderType, inputType);
            }
            else
                acceptedInputTypes = new List<Type>(new[] { inputType });

            Type stageOutputType = InputStageOutputType;
            baseType = MultiInputRecordReaderType.FindGenericBaseType(typeof(MultiInputRecordReader<>), true);
            recordType = baseType.GetGenericArguments()[0];
            if( !acceptedInputTypes.Contains(recordType) )
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The specified channel multi input record reader type {0} doesn't return objects of the correct type.", MultiInputRecordReaderType));

            List<Type> channelAcceptedInputTypes = GetMultiInputRecordReaderAcceptedInputTypes(MultiInputRecordReaderType, recordType);
            if( !channelAcceptedInputTypes.Contains(stageOutputType) )
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The specified channel multi input record reader type {0} doesn't accept objects of the correct type.", MultiInputRecordReaderType));
        }

        private static List<Type> GetMultiInputRecordReaderAcceptedInputTypes(Type multiInputRecordReaderType, Type inputType)
        {
            List<Type> acceptedInputTypes = new List<Type>();
            Attribute[] attributes = Attribute.GetCustomAttributes(multiInputRecordReaderType, typeof(InputTypeAttribute));
            if( attributes.Length == 0 )
                acceptedInputTypes.Add(inputType); // No attribute means the output type of the reader is also the only accepted input type.
            else
            {
                foreach( InputTypeAttribute attribute in attributes )
                    acceptedInputTypes.Add(attribute.AcceptedType);
            }

            return acceptedInputTypes;
        }
    }
}
