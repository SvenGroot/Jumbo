﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.Globalization;

namespace Tkl.Jumbo.Jet.Samples.FPGrowth
{
    /// <summary>
    /// Item in the feature and group list.
    /// </summary>
    public class FGListItem : Writable<FGListItem>
    {
        /// <summary>
        /// Gets or sets the feature.
        /// </summary>
        /// <value>The feature.</value>
        public Utf8String Feature { get; set; }
        /// <summary>
        /// Gets or sets the support.
        /// </summary>
        /// <value>The support.</value>
        public int Support { get; set; }
        /// <summary>
        /// Gets or sets the group id.
        /// </summary>
        /// <value>The group id.</value>
        public int GroupId { get; set; }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "[{0}, Support={1}, Group ID={2}]", Feature, Support, GroupId);
        }
    }
}
