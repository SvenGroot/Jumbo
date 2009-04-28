using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using Tkl.Jumbo.Dfs;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides information about output that a task will write to the Distributed File System.
    /// </summary>
    [XmlType(Namespace=JobConfiguration.XmlNamespace)]
    public class TaskDfsOutput : ICloneable
    {
        private string _recordWriterTypeName;
        private Type _recordWriterType;

        /// <summary>
        /// Gets or sets the path of the file to write.
        /// </summary>
        [XmlAttribute("path")]
        public string PathFormat { get; set; }

        /// <summary>
        /// Gets or sets the name of the type of <see cref="Tkl.Jumbo.IO.RecordWriter{T}"/> to use to write the file.
        /// </summary>
        [XmlAttribute("recordWriter")]
        public string RecordWriterTypeName
        {
            get { return _recordWriterTypeName; }
            set 
            {
                _recordWriterTypeName = value;
                _recordWriterType = null;
            }
        }

        /// <summary>
        /// Gets or sets the type of <see cref="Tkl.Jumbo.IO.RecordWriter{T}"/> to use to write the file.
        /// </summary>
        [XmlIgnore]
        public Type RecordWriterType
        {
            get 
            {
                if( _recordWriterType == null && _recordWriterTypeName != null )
                    _recordWriterType = Type.GetType(_recordWriterTypeName, true);
                return _recordWriterType; 
            }
            set 
            {
                _recordWriterType = value;
                _recordWriterTypeName = value == null ? null : value.AssemblyQualifiedName;
            }
        }

        /// <summary>
        /// Creates a clone of the current object.
        /// </summary>
        /// <returns>A clone of the current object.</returns>
        public TaskDfsOutput Clone()
        {
            return (TaskDfsOutput)MemberwiseClone();
        }

        /// <summary>
        /// Gets the output path for the specified task number.
        /// </summary>
        /// <param name="taskNumber">The task number.</param>
        /// <returns>The output path.</returns>
        public string GetPath(int taskNumber)
        {
            return string.Format(System.Globalization.CultureInfo.InvariantCulture, PathFormat, taskNumber);
        }

        #region ICloneable Members

        object ICloneable.Clone()
        {
            return Clone();
        }

        #endregion
    }
}
