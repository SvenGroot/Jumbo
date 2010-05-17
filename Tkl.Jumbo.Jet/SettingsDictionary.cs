// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using System.Xml;
using System.ComponentModel;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides settings for a job configuration.
    /// </summary>
    [Serializable]
    public sealed class SettingsDictionary : Dictionary<string, string>, IXmlSerializable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SettingsDictionary"/> class.
        /// </summary>
        public SettingsDictionary()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SettingsDictionary"/> class with elements that are copied from the specified <see cref="IDictionary{TKey,TValue}"/>.
        /// </summary>
        /// <param name="dictionary">The <see cref="IDictionary{TKey,TValue}"/> whose elements are copied to the <see cref="SettingsDictionary"/>.</param>
        public SettingsDictionary(IDictionary<string, string> dictionary)
            : base(dictionary)
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

        #region IXmlSerializable Members

        System.Xml.Schema.XmlSchema IXmlSerializable.GetSchema()
        {
            return null;
        }

        void IXmlSerializable.ReadXml(XmlReader reader)
        {
            if( reader == null )
                throw new ArgumentNullException("reader");
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
            if( writer == null )
                throw new ArgumentNullException("writer");
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
        /// Adds a setting with the specified type.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting.</param>
        /// <param name="value">The value of the setting.</param>
        public void AddTypedSetting<T>(string key, T value)
        {
            AddSetting(key, value);
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
            string value;
            if( TryGetValue(key, out value) )
            {
                return (T)TypeDescriptor.GetConverter(defaultValue).ConvertFrom(null, System.Globalization.CultureInfo.InvariantCulture, value);
            }
            else
                return defaultValue;
        }

        /// <summary>
        /// Tries to get a setting with the specified type.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting..</param>
        /// <param name="value">If the function returns <see langword="true"/>, receives the value of the setting.</param>
        /// <returns><see langword="true"/> if the settings dictionary contained the specified setting; otherwise, <see langword="false"/>.</returns>
        public bool TryGetTypedSetting<T>(string key, out T value)
        {
            string stringValue;
            if( TryGetValue(key, out stringValue) )
            {
                value = (T)TypeDescriptor.GetConverter(typeof(T)).ConvertFrom(null, System.Globalization.CultureInfo.InvariantCulture, stringValue);
                return true;
            }
            else
            {
                value = default(T);
                return false;
            }

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

        internal void AddSetting(string key, object value)
        {
            if( key == null )
                throw new ArgumentNullException("key");
            if( value == null )
                throw new ArgumentNullException("value");
            Add(key, (string)TypeDescriptor.GetConverter(value).ConvertTo(null, System.Globalization.CultureInfo.InvariantCulture, value, typeof(string)));
        }
    }
}
