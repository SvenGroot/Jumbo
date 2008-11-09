using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;

namespace NameServerApplication
{
    class RpcServer : MarshalByRefObject, INameServerClientProtocol, INameServerHeartbeatProtocol
    {
        private NameServer _nameServer;

        public RpcServer()
        {
            _nameServer = NameServer.Instance;
        }

        #region INameServerClientProtocol Members

        public void CreateDirectory(string path)
        {
            _nameServer.CreateDirectory(path);
        }

        public Directory GetDirectoryInfo(string path)
        {
            return _nameServer.GetDirectoryInfo(path);
        }

        public BlockAssignment CreateFile(string path)
        {
            return _nameServer.CreateFile(path);
        }

        public bool Delete(string path, bool recursive)
        {
            return _nameServer.Delete(path, recursive);
        }

        public File GetFileInfo(string path)
        {
            return _nameServer.GetFileInfo(path);
        }

        public BlockAssignment AppendBlock(string path)
        {
            return _nameServer.AppendBlock(path);
        }

        public void CloseFile(string path)
        {
            _nameServer.CloseFile(path);
        }

        public ServerAddress[] GetDataServersForBlock(Guid blockID)
        {
            return _nameServer.GetDataServersForBlock(blockID);
        }

        public bool WaitForSafeModeOff(int timeout)
        {
            return _nameServer.WaitForSafeModeOff(timeout);
        }

        public DfsMetrics GetMetrics()
        {
            return _nameServer.GetMetrics();
        }

        public bool SafeMode
        {
            get { return _nameServer.SafeMode; }
        }

        public int BlockSize
        {
            get { return _nameServer.BlockSize; }
        }

        #endregion

        #region INameServerHeartbeatProtocol Members

        public HeartbeatResponse[] Heartbeat(ServerAddress address, HeartbeatData[] data)
        {
            return _nameServer.Heartbeat(address, data);
        }

        #endregion
    }
}
