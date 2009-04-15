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
    public class SettingsDictionary : Dictionary<string, string>, IXmlSerializable
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
    }
}
