// $Id$
//
using System.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    interface ITcpChannelRecordReader
    {
        void AddSegment(int size, int number, Stream stream);
        void CompleteAdding();
    }
}
