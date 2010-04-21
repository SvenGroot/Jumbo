// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Indicates how to format the suffix when creating a string representation of an instance of the <see cref="ByteSize"/> structure.
    /// </summary>
    [Flags]
    public enum ByteSizeSuffixOptions
    {
        /// <summary>
        /// Use the default suffix format. This uses the suffixes KB, MB, GB, etc.
        /// </summary>
        None = 0,
        /// <summary>
        /// Adds a space between the number and the suffix.
        /// </summary>
        LeadingSpace = 1,
        /// <summary>
        /// Don't append a "B" to the suffix. This uses the suffixes K, M, G, etc. (or Ki, Mi, Gi etc. when combined with <see cref="UseIecSymbols"/>).
        /// </summary>
        ExcludeBytes = 1 << 1,
        /// <summary>
        /// Uses the IEC symbols for the suffixes. This uses the suffixes KiB, MiB, GiB etc.
        /// </summary>
        UseIecSymbols = 1 << 2
    }
}
