using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Attribute for task classes that indicates that the input record reader may reuse the same
    /// object instance for every record.
    /// </summary>
    [global::System.AttributeUsage(AttributeTargets.Class, Inherited = true, AllowMultiple = false)]
    sealed class AllowRecordReuseAttribute : Attribute
    {
    }
}
