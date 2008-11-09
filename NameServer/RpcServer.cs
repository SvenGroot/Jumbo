using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;

namespace NameServerApplication
{
    class RpcServer : MarshalByRefObject, INameServerClientProtocol, INameServerHeartbeatProtocol
    {

        #region INameServerClientProtocol Members

        public void CreateDirectory(string path)
        {
            NameServer.Instance.CreateDirectory(path);
        }

        public Directory GetDirectoryInfo(string path)
        {
            return NameServer.Instance.GetDirectoryInfo(path);
        }

        public BlockAssignment CreateFile(string path)
        {
            return NameServer.Instance.CreateFile(path);
        }

        public bool Delete(string path, bool recursive)
        {
            return NameServer.Instance.Delete(path, recursive);
        }

        public File GetFileInfo(string path)
        {
            return NameServer.Instance.GetFileInfo(path);
        }

        public BlockAssignment AppendBlock(string path)
        {
            return NameServer.Instance.AppendBlock(path);
        }

        public void CloseFile(string path)
        {
            NameServer.Instance.CloseFile(path);
        }

        public ServerAddress[] GetDataServersForBlock(Guid blockID)
        {
            return NameServer.Instance.GetDataServersForBlock(blockID);
        }

        public bool WaitForSafeModeOff(int timeout)
        {
            return NameServer.Instance.WaitForSafeModeOff(timeout);
        }

        public DfsMetrics GetMetrics()
        {
            return NameServer.Instance.GetMetrics();
        }

        public bool SafeMode
        {
            get { return NameServer.Instance.SafeMode; }
        }

        public int BlockSize
        {
            get { return NameServer.Instance.BlockSize; }
        }

        #endregion

        #region INameServerHeartbeatProtocol Members

        public HeartbeatResponse[] Heartbeat(ServerAddress address, HeartbeatData[] data)
        {
            return NameServer.Instance.Heartbeat(address, data);
        }

        #endregion
    }
}
