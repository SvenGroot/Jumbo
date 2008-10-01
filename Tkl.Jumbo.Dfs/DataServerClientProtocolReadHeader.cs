using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    [Serializable]
    public class DataServerClientProtocolReadHeader : DataServerClientProtocolHeader
    {
        public DataServerClientProtocolReadHeader()
        {
            base.Command = DataServerCommand.ReadBlock;
        }

        public int Offset { get; set; }
        public int Size { get; set; }
    }
}
