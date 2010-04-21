// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using System.Collections.ObjectModel;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// Represents configuration information about the input stages of a channel.
    /// </summary>
    [XmlType("ChannelInput", Namespace = JobConfiguration.XmlNamespace)]
    public sealed class ChannelInputConfiguration
    {
        private readonly ExtendedCollection<string> _inputStages = new ExtendedCollection<string>();
        private string _multiInputRecordReaderTypeName;
        private Type _multiInputRecordReaderType;

        /// <summary>
        /// Gets or sets the IDs of the stages whose tasks write to the channel.
        /// </summary>
        [XmlArrayItem("Stage")]
        public Collection<string> InputStages
        {
            get { return _inputStages; }
        }

        /// <summary>
        /// Gets or sets the name of the type of multi input record reader to use to combine the the input readers of this channel.
        /// </summary>
        [XmlAttribute("partitioner")]
        public string MultiInputRecordReaderTypeName
        {
            get { return _multiInputRecordReaderTypeName; }
            set
            {
                _multiInputRecordReaderTypeName = value;
                _multiInputRecordReaderType = null;
            }
        }

        /// <summary>
        /// Gets or sets the type of multi input record reader to use to combine the the input readers of this channel.
        /// </summary>
        [XmlIgnore]
        public Type MultiInputRecordReaderType
        {
            get
            {
                if( _multiInputRecordReaderType == null && _multiInputRecordReaderTypeName != null )
                    _multiInputRecordReaderType = Type.GetType(_multiInputRecordReaderTypeName, true);
                return _multiInputRecordReaderType;
            }
            set
            {
                _multiInputRecordReaderType = value;
                _multiInputRecordReaderTypeName = value == null ? null : value.AssemblyQualifiedName;
            }
        }
    }
}
