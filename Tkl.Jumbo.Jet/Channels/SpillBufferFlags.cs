using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Channels
{
    [Flags]
    enum SpillBufferFlags
    {
        None = 0,
        AllowRecordWrapping = 1,
        AllowMultiRecordIndexEntries = 1 << 1
    }
}
