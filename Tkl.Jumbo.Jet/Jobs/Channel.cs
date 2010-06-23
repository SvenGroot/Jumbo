// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Represents a channel between to stages in a job being built by the <see cref="OldJobBuilder"/> class.
    /// </summary>
    public sealed class Channel : IStageInput, IStageOutput
    {
        private Type _partitionerType;
        private int _partitionCount;
        private int _partitionsPerTask = 1;

        /// <summary>
        /// Gets the type of the records read from the input.
        /// </summary>
        /// <value>
        /// A <see cref="Type"/> instance for the type of the records, or <see langword="null"/> if the type hasn't been determined yet.
        /// </value>
        public Type RecordType { get; private set; }

        /// <summary>
        /// Gets or sets the stage that writes records to this channel.
        /// </summary>
        /// <value>The sending stage, or <see langword="null"/> if none has been attached yet.</value>
        public StageBuilder SendingStage { get; private set; }

        /// <summary>
        /// Gets or sets the stage that reads records from this channel.
        /// </summary>
        /// <value>The receiving stage, or <see langword="null"/> if none has been attached yet.</value>
        public StageBuilder ReceivingStage { get; private set; }

        /// <summary>
        /// Gets or sets the partitioner to use to spread the records across the output tasks.
        /// </summary>
        /// <value>
        /// The <see cref="Type"/> of the partitioner to use, or <see langword="null"/> to use to the default hash partitioner. The default value is <see langword="null"/>.
        /// </value>
        public Type PartitionerType
        {
            get { return _partitionerType; }
            set
            {
                if( value != null )
                {
                    Type paritionerInterfaceType = value.FindGenericInterfaceType(typeof(IPartitioner<>), true);
                    if( !CheckRecordType(paritionerInterfaceType.GetGenericArguments()[0]) )
                        throw new ArgumentException("The partitioner's record type doesn't match the channel's record type.");
                }
                _partitionerType = value;
            }
        }

        /// <summary>
        /// Gets or sets the channel type.
        /// </summary>
        /// <value>
        /// The channel type, or <see langword="null"/> to let the <see cref="OldJobBuilder"/> decide. The default value is <see langword="null"/>.
        /// </value>
        public ChannelType? ChannelType { get; set; }

        /// <summary>
        /// Gets or sets the number of partitions to create
        /// </summary>
        /// <value>
        /// The number of partitions to create, or zero to let the <see cref="OldJobBuilder"/> decide. The default value is zero.
        /// </value>
        public int PartitionCount
        {
            get { return _partitionCount; }
            set
            {
                if( value < 0 )
                    throw new ArgumentOutOfRangeException("value", "The partition count must be 0 or higher.");
                if( value > 0 && value % _partitionsPerTask != 0 )
                    throw new InvalidOperationException("The total number of partitions must be divisible by the number of partitions per task.");
                _partitionCount = value;
            }
        }

        /// <summary>
        /// Gets or sets the number of partitions per task.
        /// </summary>
        /// <value>The number of partitions per task. The default value is one.</value>
        public int PartitionsPerTask
        {
            get { return _partitionsPerTask; }
            set
            {
                if( value < 1 )
                    throw new ArgumentOutOfRangeException("value", "The pnumber of partitions per task must be 1 or higher.");
                if( _partitionCount > 0 && _partitionCount % value != 0 )
                    throw new InvalidOperationException("The total number of partitions must be divisible by the number of partitions per task.");

                _partitionsPerTask = value;
            }
        }

        /// <summary>
        /// Gets or sets the method used to assign partitions to tasks when the job is started.
        /// </summary>
        /// <value>The partition assignment method.</value>
        public PartitionAssignmentMethod PartitionAssignmentMethod { get; set; }

        internal Type MultiInputRecordReaderType { get; set; }

        Type IStageInput.RecordType
        {
            get { return RecordType; }
        }

        Type IStageOutput.RecordType
        {
            get { return RecordType; }
        }

        internal void AttachSendingStage(StageBuilder stage)
        {
            if( SendingStage != null )
                throw new InvalidOperationException("This channel already has a sending stage.");
            if( !CheckRecordType(stage.OutputRecordType) )
                throw new ArgumentException("The stage's output record type does not match this channel's record type.", "stage");

            SendingStage = stage;
        }

        internal void AttachReceivingStage(StageBuilder stage)
        {
            if( ReceivingStage != null )
                throw new InvalidOperationException("This channel already has a receiving stage.");
            if( !CheckRecordType(stage.InputRecordType) )
                throw new ArgumentException("The stage's input record type does not match this channel's record type.", "stage");

            ReceivingStage = stage;
        }

        private bool CheckRecordType(Type recordType)
        {
            if( RecordType == null )
            {
                RecordType = recordType;
                return true;
            }
            else
                return RecordType == recordType;
        }
    }
}
