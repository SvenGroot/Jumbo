using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    [Serializable]
    public class NewBlockHeartbeatData : HeartbeatData
    {
        public Guid BlockID { get; set; }
        public int Size { get; set; }
    }
}
