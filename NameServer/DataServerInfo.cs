using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo;

namespace NameServerApplication
{
    class DataServerInfo
    {
        private readonly List<Guid> _blocksToDelete = new List<Guid>();
        private readonly List<Guid> _blocks = new List<Guid>();

        public DataServerInfo(string hostName, int port)
        {
            Address = new ServerAddress(hostName, port);
        }

        public DataServerInfo(ServerAddress address)
        {
            Address = address;
        }

        public ServerAddress Address { get; private set; }

        public bool HasReportedBlocks { get; set; }

        public List<Guid> Blocks { get { return _blocks; } }

        public DateTime LastContactUtc { get; set; }

        public void AddBlockToDelete(Guid blockID)
        {
            lock( _blocksToDelete )
                _blocksToDelete.Add(blockID);
        }

        public Guid[] GetAndClearBlocksToDelete()
        {
            lock( _blocksToDelete )
            {
                Guid[] result = _blocksToDelete.ToArray();
                _blocksToDelete.Clear();
                return result;
            }
        }
    }
}
