// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Globalization;
using System.ComponentModel;

namespace Tkl.Jumbo.Test
{
    [TestFixture]
    public class BinaryValueTests
    {
        [Test]
        public void TestParse()
        {
            Assert.AreEqual(new BinaryValue(123), BinaryValue.Parse("123", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(123), BinaryValue.Parse("123B", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(125952), BinaryValue.Parse("123KB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(125952), BinaryValue.Parse("123KiB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(125952), BinaryValue.Parse("123K", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(128974848), BinaryValue.Parse("123MB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(128974848), BinaryValue.Parse("123MiB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(128974848), BinaryValue.Parse("123M", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(132070244352), BinaryValue.Parse("123GB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(132070244352), BinaryValue.Parse("123GiB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(132070244352), BinaryValue.Parse("123G", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(135239930216448), BinaryValue.Parse("123TB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(135239930216448), BinaryValue.Parse("123TiB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(135239930216448), BinaryValue.Parse("123T", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(138485688541642752), BinaryValue.Parse("123PB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(138485688541642752), BinaryValue.Parse("123PiB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(138485688541642752), BinaryValue.Parse("123P", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(138485688541642752), BinaryValue.Parse("123 PB ", CultureInfo.InvariantCulture)); // with some spaces.

            // Explicit culture test:
            Assert.AreEqual(new BinaryValue(126464), BinaryValue.Parse("123.5KB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new BinaryValue(126464), BinaryValue.Parse("123,5KB", new CultureInfo("nl-NL")));
            // Test version without provider uses current culture (weak test but it'll do)
            string size = string.Format(CultureInfo.CurrentCulture, "{0:0.0}KB", 123.5);
            Assert.AreEqual(new BinaryValue(126464), BinaryValue.Parse(size));
        }

        [Test]
        public void TestToString()
        {
            BinaryValue target = new BinaryValue(123456789012345678);
            Assert.AreEqual("123456789012345678B", target.ToString(CultureInfo.InvariantCulture));
            Assert.AreEqual("120563270519868.826171875KB", target.ToString("KB", CultureInfo.InvariantCulture));
            Assert.AreEqual("120563270519868.826171875KiB", target.ToString("KiB", CultureInfo.InvariantCulture));
            Assert.AreEqual("120563270519868.826171875K", target.ToString("K", CultureInfo.InvariantCulture));
            Assert.AreEqual("117737568867.05940055847167969MB", target.ToString("MB", CultureInfo.InvariantCulture)); // Rounded due to formatting
            Assert.AreEqual("117737568867.05940055847167969MiB", target.ToString("MiB", CultureInfo.InvariantCulture)); // Rounded due to formatting
            Assert.AreEqual("117737568867.05940055847167969M", target.ToString("M", CultureInfo.InvariantCulture)); // Rounded due to formatting
            Assert.AreEqual("114978094.59673769585788249969GB", target.ToString("GB", CultureInfo.InvariantCulture)); // Rounded due to formatting
            Assert.AreEqual("114978094.59673769585788249969GiB", target.ToString("GiB", CultureInfo.InvariantCulture)); // Rounded due to formatting
            Assert.AreEqual("114978094.59673769585788249969G", target.ToString("G", CultureInfo.InvariantCulture)); // Rounded due to formatting
            Assert.AreEqual("112283.29550462665611121337861TB", target.ToString("TB", CultureInfo.InvariantCulture)); // Rounded due to fommatting
            Assert.AreEqual("112283.29550462665611121337861TiB", target.ToString("TiB", CultureInfo.InvariantCulture)); // Rounded due to fommatting
            Assert.AreEqual("112283.29550462665611121337861T", target.ToString("T", CultureInfo.InvariantCulture)); // Rounded due to fommatting
            Assert.AreEqual("109.65165576623696885860681505PB", target.ToString("PB", CultureInfo.InvariantCulture)); // Rounded due to fommatting
            Assert.AreEqual("109.65165576623696885860681505PiB", target.ToString("PiB", CultureInfo.InvariantCulture)); // Rounded due to fommatting
            Assert.AreEqual("109.65165576623696885860681505P", target.ToString("P", CultureInfo.InvariantCulture)); // Rounded due to fommatting

            Assert.AreEqual("109.65165576623696885860681505 PB", target.ToString(" PB", CultureInfo.InvariantCulture)); // Rounded due to fommatting, with a space

            // Explicit format test:
            Assert.AreEqual("109.7 PB", target.ToString("0.# PB", CultureInfo.InvariantCulture));

            // Explicit culture test:
            Assert.AreEqual("109,7PB", target.ToString("0.#PB", new CultureInfo("nl-NL")));
            Assert.AreEqual("109,65165576623696885860681505PB", target.ToString("PB", new CultureInfo("nl-NL"))); // Rounded due to formatting

            // Current culture test:
            Assert.AreEqual(target.ToString("PB", CultureInfo.CurrentCulture), target.ToString("PB"));

            // Automatic units test:
            Assert.AreEqual("123B", ((BinaryValue)123).ToString("AB", CultureInfo.InvariantCulture));
            Assert.AreEqual("123B", ((BinaryValue)123).ToString("SB", CultureInfo.InvariantCulture));
            Assert.AreEqual("126464B", ((BinaryValue)126464).ToString("AB", CultureInfo.InvariantCulture));
            Assert.AreEqual("123.5KB", ((BinaryValue)126464).ToString("SB", CultureInfo.InvariantCulture));
            Assert.AreEqual("126464KB", ((BinaryValue)129499136).ToString("AB", CultureInfo.InvariantCulture));
            Assert.AreEqual("123.5MB", ((BinaryValue)129499136).ToString("SB", CultureInfo.InvariantCulture));
            Assert.AreEqual("126464MB", ((BinaryValue)132607115264).ToString("AB", CultureInfo.InvariantCulture));
            Assert.AreEqual("123.5GB", ((BinaryValue)132607115264).ToString("SB", CultureInfo.InvariantCulture));
            Assert.AreEqual("126464GB", ((BinaryValue)135789686030336).ToString("AB", CultureInfo.InvariantCulture));
            Assert.AreEqual("123.5TB", ((BinaryValue)135789686030336).ToString("SB", CultureInfo.InvariantCulture));
            Assert.AreEqual("126464TB", ((BinaryValue)139048638495064064).ToString("AB", CultureInfo.InvariantCulture));
            Assert.AreEqual("123.5PB", ((BinaryValue)139048638495064064).ToString("SB", CultureInfo.InvariantCulture));
            Assert.AreEqual("123456789012345678B", ((BinaryValue)123456789012345678).ToString("AB", CultureInfo.InvariantCulture));
            Assert.AreEqual("109.7PB", ((BinaryValue)123456789012345678).ToString("0.#SB", CultureInfo.InvariantCulture));

            // Test with different options:
            Assert.AreEqual("126464", ((BinaryValue)126464).ToString("A", CultureInfo.InvariantCulture));
            Assert.AreEqual("123.5K", ((BinaryValue)126464).ToString("S", CultureInfo.InvariantCulture));
            Assert.AreEqual("126464 B", ((BinaryValue)126464).ToString(" AiB", CultureInfo.InvariantCulture));
            Assert.AreEqual("123.5 KiB", ((BinaryValue)126464).ToString(" SiB", CultureInfo.InvariantCulture));

            // Test defaults, should have same effect as AB.
            string expected = 126464.ToString() + "KB";
            Assert.AreEqual(expected, ((BinaryValue)129499136).ToString());
            Assert.AreEqual(expected, ((BinaryValue)129499136).ToString((IFormatProvider)null));
            Assert.AreEqual(expected, ((BinaryValue)129499136).ToString(CultureInfo.CurrentCulture));
            Assert.AreEqual(expected, ((BinaryValue)129499136).ToString(null, null));
            Assert.AreEqual(expected, ((BinaryValue)129499136).ToString(""));
            Assert.AreEqual(expected, ((BinaryValue)129499136).ToString("", null));

            // Test IFormattable
            Assert.AreEqual("test 109.7 PB test2", string.Format(CultureInfo.InvariantCulture, "test {0:0.# SB} test2", ((BinaryValue)123456789012345678)));
        }


        [Test]
        public void TestEquality()
        {
            Assert.AreEqual(new BinaryValue(123), new BinaryValue(123));
            Assert.AreNotEqual(new BinaryValue(123), new BinaryValue(124));
            Assert.IsTrue(new BinaryValue(123) == new BinaryValue(123));
            Assert.IsFalse(new BinaryValue(123) == new BinaryValue(124));
            Assert.IsTrue(new BinaryValue(123) != new BinaryValue(124));
            Assert.IsFalse(new BinaryValue(123) != new BinaryValue(123));
        }

        [Test]
        public void TestTypeConverter()
        {
            TypeConverter converter = TypeDescriptor.GetConverter(typeof(BinaryValue));
            BinaryValue target = new BinaryValue(125952);
            Assert.AreEqual(target, converter.ConvertFrom(null, CultureInfo.InvariantCulture, "123KB"));
            Assert.AreEqual("123KB", converter.ConvertTo(null, CultureInfo.InvariantCulture, target, typeof(string)));
            target = new BinaryValue(129499136);
            Assert.AreEqual(target, converter.ConvertFrom(null, CultureInfo.InvariantCulture, "123.5MB"));
            Assert.AreEqual("126464KB", converter.ConvertTo(null, CultureInfo.InvariantCulture, target, typeof(string)));
        }
    }
}
