using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using System.IO;

namespace DataServer
{
    static class ExtensionMethods
    {
        public static void WriteResult(this BinaryWriter writer, DataServerClientProtocolResult result)
        {
            writer.Write((int)result);
        }
    }
}
