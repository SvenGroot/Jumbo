using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Represents configuration information about a channel through which two tasks communicate.
    /// </summary>
    [XmlType("Channel", Namespace=JobConfiguration.XmlNamespace)]
    public class ChannelConfiguration
    {
        private List<TaskConfiguration> _inputTaskConfigs;
        private List<TaskConfiguration> _outputTaskConfigs;
        private string[] _inputTasks;
        private string[] _outputTasks;

        /// <summary>
        /// Gets or sets the type of the channel.
        /// </summary>
        [XmlAttribute("type")]
        public ChannelType ChannelType { get; set; }

        /// <summary>
        /// Gets or sets the IDs of the tasks that write to the channel.
        /// </summary>
        [XmlArrayItem("Task")]
        public string[] InputTasks
        {
            get
            {
                if( _inputTasks == null && _inputTaskConfigs != null )
                    _inputTasks = (from t in _inputTaskConfigs select t.TaskID).ToArray();
                return _inputTasks;
            }
            set
            {
                _inputTasks = value;
                _inputTaskConfigs = null;
            }
        }

        /// <summary>
        /// Gets or sets the IDs of the tasks that read from the channel.
        /// </summary>
        [XmlArrayItem("Task")]
        public string[] OutputTasks
        {
            get
            {
                if( _outputTasks == null && _outputTaskConfigs != null )
                    _outputTasks = (from t in _outputTaskConfigs select t.TaskID).ToArray();
                return _outputTasks;
            }
            set
            {
                _outputTasks = value;
                _outputTaskConfigs = null;
            }
        }

        /// <summary>
        /// Gets or sets a value that indicates whether the file channel should always use TCP downloads.
        /// </summary>
        /// <value>
        /// For a <see cref="ChannelType"/> value of <see cref="Tkl.Jumbo.Jet.Channels.ChannelType.File"/>, <see langword="true"/>
        /// to indicate that it should always use TCP to download the files even if the input task is on the same physical
        /// host as the output task; <see langword="false"/> to indicate it should access the output file directly if the
        /// input task is local. This property has no effect for other types of channels.
        /// </value>
        /// <remarks>
        /// This property is primarily used for testing of the TCP server.
        /// </remarks>
        [XmlAttribute("forceFileDownload")]
        public bool ForceFileDownload { get; set; }

        /// <summary>
        /// Gets or sets the type name of a class implementing <see cref="Tkl.Jumbo.IO.IPartitioner{T}"/> to use as the
        /// partitioner.
        /// </summary>
        /// <remarks>
        /// You do not need to set this property if their is only one output task.
        /// </remarks>
        [XmlAttribute("partitionerType")]
        public string PartitionerType { get; set; }

        internal void AddInputTask(TaskConfiguration task)
        {
            if( _inputTaskConfigs == null )
                _inputTaskConfigs = new List<TaskConfiguration>();
            _inputTaskConfigs.Add(task);
            task.OutputChannel = this;
            _inputTasks = null;
        }

        internal void AddInputTasks(IEnumerable<TaskConfiguration> tasks)
        {
            if( _inputTaskConfigs == null )
                _inputTaskConfigs = new List<TaskConfiguration>();
            _inputTaskConfigs.AddRange(tasks);
            foreach( TaskConfiguration task in tasks )
            {
                task.OutputChannel = this;
            }
            _inputTasks = null;
        }

        internal void RemoveInputTasks(IEnumerable<TaskConfiguration> tasks)
        {
            if( _inputTaskConfigs != null )
            {
                foreach( TaskConfiguration task in tasks )
                {
                    task.OutputChannel = null;
                    _inputTaskConfigs.Remove(task);
                }
                _inputTasks = null;
            }
        }

        internal void ClearInputTasks()
        {
            if( _inputTaskConfigs != null )
            {
                foreach( TaskConfiguration task in _inputTaskConfigs )
                    task.OutputChannel = null;
                _inputTaskConfigs.Clear();
                _inputTasks = null;
            }
        }
        
        internal void AddOutputTask(TaskConfiguration task)
        {
            if( _outputTaskConfigs == null )
                _outputTaskConfigs = new List<TaskConfiguration>();
            _outputTaskConfigs.Add(task);
            task.InputChannel = this;
            _outputTasks = null;
        }
        
        internal void AddOutputTasks(IEnumerable<TaskConfiguration> tasks)
        {
            if( _outputTaskConfigs == null )
                _outputTaskConfigs = new List<TaskConfiguration>();
            _outputTaskConfigs.AddRange(tasks);
            foreach( TaskConfiguration task in tasks )
            {
                task.InputChannel = this;
            }
            _outputTasks = null;
        }

        internal void ClearOutputTasks()
        {
            if( _outputTaskConfigs != null )
            {
                foreach( TaskConfiguration task in _outputTaskConfigs )
                    task.InputChannel = null;
                _outputTaskConfigs.Clear();
                _outputTasks = null;
            }
        }

        internal void RemoveOutputTasks(IEnumerable<TaskConfiguration> tasks)
        {
            if( _outputTaskConfigs != null )
            {
                foreach( TaskConfiguration task in tasks )
                {
                    task.InputChannel = null;
                    _outputTaskConfigs.Remove(task);
                }
                _outputTasks = null;
            }
        }
    }
}
