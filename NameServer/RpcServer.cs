using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo;

namespace NameServerApplication
{
    class RpcServer : MarshalByRefObject, INameServerClientProtocol, INameServerHeartbeatProtocol
    {

        #region INameServerClientProtocol Members

        public void CreateDirectory(string path)
        {
            NameServer.Instance.CreateDirectory(path);
        }

        public DfsDirectory GetDirectoryInfo(string path)
        {
            return NameServer.Instance.GetDirectoryInfo(path);
        }

        public BlockAssignment CreateFile(string path, int blockSize)
        {
            return NameServer.Instance.CreateFile(path, blockSize);
        }

        public bool Delete(string path, bool recursive)
        {
            return NameServer.Instance.Delete(path, recursive);
        }

        public void Move(string from, string to)
        {
            NameServer.Instance.Move(from, to);
        }

        public DfsFile GetFileInfo(string path)
        {
            return NameServer.Instance.GetFileInfo(path);
        }

        public FileSystemEntry GetFileSystemEntryInfo(string path)
        {
            return NameServer.Instance.GetFileSystemEntryInfo(path);
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
            set { NameServer.Instance.SafeMode = value; }
        }

        public int BlockSize
        {
            get { return NameServer.Instance.BlockSize; }
        }

        public int GetDataServerBlockCount(ServerAddress dataServer, Guid[] blocks)
        {
            return NameServer.Instance.GetDataServerBlockCount(dataServer, blocks);
        }

        public Guid[] GetDataServerBlocks(ServerAddress dataServer)
        {
            return NameServer.Instance.GetDataServerBlocks(dataServer);
        }

        public string GetLogFileContents(int maxSize)
        {
            return NameServer.Instance.GetLogFileContents(maxSize);
        }

        public void RemoveDataServer(ServerAddress dataServer)
        {
            NameServer.Instance.RemoveDataServer(dataServer);
        }

        public void CreateCheckpoint()
        {
            NameServer.Instance.CreateCheckpoint();
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
