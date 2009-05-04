using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using System.Xml;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides settings for a job configuration.
    /// </summary>
    [Serializable]
    public sealed class SettingsDictionary : Dictionary<string, string>, IXmlSerializable
    {
        #region IXmlSerializable Members

        System.Xml.Schema.XmlSchema IXmlSerializable.GetSchema()
        {
            return null;
        }

        void IXmlSerializable.ReadXml(XmlReader reader)
        {
            string startElementName = reader.Name;
            int depth = reader.Depth;
            if( reader.IsEmptyElement )
            {
                reader.ReadStartElement();
                return;
            }

            reader.ReadStartElement();
            while( !(reader.NodeType == XmlNodeType.EndElement && reader.Name == startElementName && reader.Depth == depth) )
            {
                if( reader.IsStartElement("Setting", JobConfiguration.XmlNamespace) )
                    Add(reader.GetAttribute("key"), reader.GetAttribute("value"));
                reader.Read();
            }
            reader.ReadEndElement();
        }

        void IXmlSerializable.WriteXml(XmlWriter writer)
        {
            foreach( var item in this )
            {
                writer.WriteStartElement("Setting", JobConfiguration.XmlNamespace);
                writer.WriteAttributeString("key", item.Key);
                writer.WriteAttributeString("value", item.Value);
                writer.WriteEndElement();
            }
        }

        #endregion

        /// <summary>
        /// Gets a setting with the specified type and default value.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting.</param>
        /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary"/>.</param>
        /// <returns>The value of the setting, or <paramref name="defaultValue"/> if the setting was not present in the <see cref="SettingsDictionary"/>.</returns>
        public T GetTypedSetting<T>(string key, T defaultValue)
        {
            string value;
            if( TryGetValue(key, out value) )
            {
                if( typeof(T).IsEnum )
                    return (T)Enum.Parse(typeof(T), value);
                else
                    return (T)Convert.ChangeType(value, typeof(T), System.Globalization.CultureInfo.InvariantCulture);
            }
            else
                return defaultValue;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SettingsDictionary"/> class.
        /// </summary>
        public SettingsDictionary()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SettingsDictionary"/> class with serialized data.
        /// </summary>
        /// <param name="info">A <see cref="System.Runtime.Serialization.SerializationInfo"/> object containing the information required to serialize the <see cref="SettingsDictionary"/>.</param>
        /// <param name="context">A <see cref="System.Runtime.Serialization.StreamingContext"/> structure containing the source and destination of the serialized stream associated with the <see cref="SettingsDictionary"/>.</param>
        private SettingsDictionary(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context)
            : base(info, context)
        {
        }

        /// <summary>
        /// Gets a string setting with the specified default value.
        /// </summary>
        /// <param name="key">The name of the setting.</param>
        /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary"/>.</param>
        /// <returns>The value of the setting, or <paramref name="defaultValue"/> if the setting was not present in the <see cref="SettingsDictionary"/>.</returns>
        public string GetSetting(string key, string defaultValue)
        {
            string value;
            if( TryGetValue(key, out value) )
                return value;
            else
                return defaultValue;
        }
    }
}
