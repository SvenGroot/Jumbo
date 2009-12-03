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
    public class ByteSizeTests
    {
        [Test]
        public void TestParse()
        {
            Assert.AreEqual(new ByteSize(123), ByteSize.Parse("123", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(123), ByteSize.Parse("123B", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(125952), ByteSize.Parse("123KB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(125952), ByteSize.Parse("123KiB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(125952), ByteSize.Parse("123K", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(128974848), ByteSize.Parse("123MB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(128974848), ByteSize.Parse("123MiB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(128974848), ByteSize.Parse("123M", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(132070244352), ByteSize.Parse("123GB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(132070244352), ByteSize.Parse("123GiB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(132070244352), ByteSize.Parse("123G", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(135239930216448), ByteSize.Parse("123TB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(135239930216448), ByteSize.Parse("123TiB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(135239930216448), ByteSize.Parse("123T", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(138485688541642752), ByteSize.Parse("123PB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(138485688541642752), ByteSize.Parse("123PiB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(138485688541642752), ByteSize.Parse("123P", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(138485688541642752), ByteSize.Parse("123 PB ", CultureInfo.InvariantCulture)); // with some spaces.

            // Explicit culture test:
            Assert.AreEqual(new ByteSize(126464), ByteSize.Parse("123.5KB", CultureInfo.InvariantCulture));
            Assert.AreEqual(new ByteSize(126464), ByteSize.Parse("123,5KB", new CultureInfo("nl-NL")));
            // Test version without provider uses current culture (weak test but it'll do)
            string size = string.Format(CultureInfo.CurrentCulture, "{0:0.0}KB", 123.5);
            Assert.AreEqual(new ByteSize(126464), ByteSize.Parse(size));
        }

        [Test]
        public void TestToString()
        {
            ByteSize target = new ByteSize(123456789012345678);
            Assert.AreEqual("123456789012345678", target.ToString(CultureInfo.InvariantCulture));
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
            Assert.AreEqual("109.7PB", target.ToString("0.#", "PB", CultureInfo.InvariantCulture));

            // Explicit culture test:
            Assert.AreEqual("109,7PB", target.ToString("0.#", "PB", new CultureInfo("nl-NL")));
            Assert.AreEqual("109,65165576623696885860681505PB", target.ToString("PB", new CultureInfo("nl-NL"))); // Rounded due to fommatting

            // Current culture test:
            Assert.AreEqual(target.ToString("PB", CultureInfo.CurrentCulture), target.ToString("PB"));
        }

        [Test]
        public void TestToShortString()
        {
            Assert.AreEqual("123B", new ByteSize(123).ToShortString(null, ByteSizeSuffixOptions.None, CultureInfo.InvariantCulture));
            Assert.AreEqual("123KB", new ByteSize(125952).ToShortString(null, ByteSizeSuffixOptions.None, CultureInfo.InvariantCulture));
            Assert.AreEqual("123MB", new ByteSize(128974848).ToShortString(null, ByteSizeSuffixOptions.None, CultureInfo.InvariantCulture));
            Assert.AreEqual("123GB", new ByteSize(132070244352).ToShortString(null, ByteSizeSuffixOptions.None, CultureInfo.InvariantCulture));
            Assert.AreEqual("123TB", new ByteSize(135239930216448).ToShortString(null, ByteSizeSuffixOptions.None, CultureInfo.InvariantCulture));
            Assert.AreEqual("123PB", new ByteSize(138485688541642752).ToShortString(null, ByteSizeSuffixOptions.None, CultureInfo.InvariantCulture));
            Assert.AreEqual("109.65165576623696885860681505PB", new ByteSize(123456789012345678).ToShortString(null, ByteSizeSuffixOptions.None, CultureInfo.InvariantCulture));

            // Test with different options:
            Assert.AreEqual("123K", new ByteSize(125952).ToShortString(null, ByteSizeSuffixOptions.ExcludeBytes, CultureInfo.InvariantCulture));
            Assert.AreEqual("123 KiB", new ByteSize(125952).ToShortString(null, ByteSizeSuffixOptions.LeadingSpace | ByteSizeSuffixOptions.UseIecSymbols, CultureInfo.InvariantCulture));

            // Test with explicit format:
            Assert.AreEqual("109.7PB", new ByteSize(123456789012345678).ToShortString("0.#", ByteSizeSuffixOptions.None, CultureInfo.InvariantCulture));

            // Test with explicit culture:
            Assert.AreEqual("109,7PB", new ByteSize(123456789012345678).ToShortString("0.#", ByteSizeSuffixOptions.None, new CultureInfo("nl-NL")));

            // Test defaults:
            Assert.AreEqual(new ByteSize(123456789012345678).ToShortString(null, ByteSizeSuffixOptions.None, CultureInfo.CurrentCulture), new ByteSize(123456789012345678).ToShortString());
        }

        [Test]
        public void TestEquality()
        {
            Assert.AreEqual(new ByteSize(123), new ByteSize(123));
            Assert.AreNotEqual(new ByteSize(123), new ByteSize(124));
            Assert.IsTrue(new ByteSize(123) == new ByteSize(123));
            Assert.IsFalse(new ByteSize(123) == new ByteSize(124));
            Assert.IsTrue(new ByteSize(123) != new ByteSize(124));
            Assert.IsFalse(new ByteSize(123) != new ByteSize(123));
        }

        [Test]
        public void TestTypeConverter()
        {
            TypeConverter converter = TypeDescriptor.GetConverter(typeof(ByteSize));
            ByteSize target = new ByteSize(125952);
            Assert.AreEqual(target, converter.ConvertFrom(null, CultureInfo.InvariantCulture, "123KB"));
            Assert.AreEqual("125952", converter.ConvertTo(null, CultureInfo.InvariantCulture, target, typeof(string)));
        }
    }
}
