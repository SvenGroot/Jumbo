using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Jet.Channels;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides configuration information about a task.
    /// </summary>
    [XmlType("Task", Namespace=JobConfiguration.XmlNamespace)]
    public class TaskConfiguration : ICloneable
    {
        private Type _type;
        private string _typeName;

        /// <summary>
        /// Gets or sets the unique identifier for this task.
        /// </summary>
        [XmlAttribute("id")]
        public string TaskID { get; set; }

        /// <summary>
        /// Gets or sets the task type.
        /// </summary>
        [XmlIgnore]
        public Type TaskType
        {
            get 
            {
                if( _type == null && _typeName != null )
                    _type = Type.GetType(_typeName);
                return _type; 
            }
            set
            {
                _type = value;
                _typeName = _type == null ? null : _type.AssemblyQualifiedName;
            }
        }
        
        /// <summary>
        /// Gets or sets the name of the type implementing this task.
        /// </summary>
        [XmlAttribute("type")]
        public string TypeName
        {
            get { return _typeName; }
            set
            {
                _typeName = value;
                _type = null;
            }
        }

        /// <summary>
        /// Gets or sets the profiling options for the task.
        /// </summary>
        /// <remarks>
        /// <note>
        ///   Profiling is supported only under Mono.
        /// </note>
        /// <para>
        ///   This property can be set to a comma-separated list of options for the Mono default profiler
        ///   (see the man page for mono for more details). Leave it empty to disable profiling.
        /// </para>
        /// </remarks>
        [XmlAttribute("profileOptions")]
        public string ProfileOptions { get; set; }

        /// <summary>
        /// Gets or sets the input from the distributed file system for this task.
        /// </summary>
        public TaskDfsInput DfsInput { get; set; }

        /// <summary>
        /// Gets or sets the output to the distributed file system for this task.
        /// </summary>
        public TaskDfsOutput DfsOutput { get; set; }

        /// <summary>
        /// Gets or sets a list of settings that are specific to this task.
        /// </summary>
        public SettingsDictionary TaskSettings { get; set; }

        /// <summary>
        /// Gets or sets the name of the stage that this task belongs to. This property is not serialized.
        /// </summary>
        [XmlIgnore]
        public string Stage { get; set; }

        /// <summary>
        /// Creates a clone of the current object.
        /// </summary>
        /// <returns>A clone of the current object.</returns>
        public TaskConfiguration Clone()
        {
            TaskConfiguration clone = (TaskConfiguration)MemberwiseClone();
            if( DfsInput != null )
                clone.DfsInput = DfsInput.Clone();
            if( DfsOutput != null )
                clone.DfsOutput = DfsOutput.Clone();
            clone.InputChannel = null;
            clone.OutputChannel = null;
            return clone;
        }

        /// <summary>
        /// Gets a setting with the specified type and default value.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting.</param>
        /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary"/>.</param>
        /// <returns>The value of the setting, or <paramref name="defaultValue"/> if the setting was not present in the <see cref="SettingsDictionary"/>.</returns>
        public T GetTypedSetting<T>(string key, T defaultValue)
        {
            if( TaskSettings == null )
                return defaultValue;
            else
                return TaskSettings.GetTypedSetting(key, defaultValue);
        }

        /// <summary>
        /// Gets a string setting with the specified default value.
        /// </summary>
        /// <param name="key">The name of the setting.</param>
        /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary"/>.</param>
        /// <returns>The value of the setting, or <paramref name="defaultValue"/> if the setting was not present in the <see cref="SettingsDictionary"/>.</returns>
        public string GetSetting(string key, string defaultValue)
        {
            if( TaskSettings == null )
                return defaultValue;
            else
                return TaskSettings.GetSetting(key, defaultValue);
        }

        /// <summary>
        /// Adds a setting.
        /// </summary>
        /// <param name="key">The name of the setting.</param>
        /// <param name="value">The value of the setting.</param>
        public void AddSetting(string key, string value)
        {
            if( TaskSettings == null )
                TaskSettings = new SettingsDictionary();
            TaskSettings.Add(key, value);
        }

        /// <summary>
        /// Adds a setting with the specified type.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting.</param>
        /// <param name="value">The value of the setting.</param>
        public void AddTypedSetting<T>(string key, T value)
        {
            AddSetting(key, Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture));
        }

        #region ICloneable Members

        object ICloneable.Clone()
        {
            return Clone();
        }

        #endregion

        [XmlIgnore]
        internal ChannelConfiguration InputChannel { get; set; }

        [XmlIgnore]
        internal ChannelConfiguration OutputChannel { get; set; }
    }
}
