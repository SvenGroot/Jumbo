// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo;
using Tkl.Jumbo.IO;
using Tkl.Jumbo.Dfs.FileSystem;

namespace NameServerApplication
{
    class RpcServer : MarshalByRefObject, INameServerClientProtocol, INameServerHeartbeatProtocol
    {

        #region INameServerClientProtocol Members

        public void CreateDirectory(string path)
        {
            NameServer.Instance.CreateDirectory(path);
        }

        public JumboDirectory GetDirectoryInfo(string path)
        {
            return NameServer.Instance.GetDirectoryInfo(path);
        }

        public BlockAssignment CreateFile(string path, int blockSize, int replicationFactor, bool useLocalReplica, RecordStreamOptions recordOptions)
        {
            return NameServer.Instance.CreateFile(path, blockSize, replicationFactor, useLocalReplica, recordOptions);
        }

        public bool Delete(string path, bool recursive)
        {
            return NameServer.Instance.Delete(path, recursive);
        }

        public void Move(string from, string to)
        {
            NameServer.Instance.Move(from, to);
        }

        public JumboFile GetFileInfo(string path)
        {
            return NameServer.Instance.GetFileInfo(path);
        }

        public JumboFileSystemEntry GetFileSystemEntryInfo(string path)
        {
            return NameServer.Instance.GetFileSystemEntryInfo(path);
        }

        public BlockAssignment AppendBlock(string path, bool useLocalReplica)
        {
            return NameServer.Instance.AppendBlock(path, useLocalReplica);
        }

        public void CloseFile(string path)
        {
            NameServer.Instance.CloseFile(path);
        }

        public ServerAddress[] GetDataServersForBlock(Guid blockID)
        {
            return NameServer.Instance.GetDataServersForBlock(blockID);
        }

        public string GetFileForBlock(Guid blockId)
        {
            return NameServer.Instance.GetFileForBlock(blockId);
        }

        public Guid[] GetBlocks(BlockKind kind)
        {
            return NameServer.Instance.GetBlocks(kind);
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

        public Guid[] GetDataServerBlocksFromList(ServerAddress dataServer, Guid[] blocks)
        {
            return NameServer.Instance.GetDataServerBlocksFromList(dataServer, blocks);
        }

        public string GetLogFileContents(LogFileKind kind, int maxSize)
        {
            return NameServer.Instance.GetLogFileContents(kind, maxSize);
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
