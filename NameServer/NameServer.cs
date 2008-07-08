using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.Dfs;

namespace NameServer
{
    /// <summary>
    /// RPC server for the NameServer.
    /// </summary>
    class NameServer : MarshalByRefObject, IClientProtocol
    {
        private FileSystem _fileSystem = new FileSystem(true);

        public override object InitializeLifetimeService()
        {
            // This causes the object to live forever.
            return null;
        }

        #region IClientProtocol Members

        public void CreateDirectory(string path)
        {
            _fileSystem.CreateDirectory(path);
        }

        public Directory GetDirectoryInfo(string path)
        {
            return _fileSystem.GetDirectoryInfo(path);
        }


        public void CreateFile(string path)
        {
            _fileSystem.CreateFile(path);
        }

        public File GetFileInfo(string path)
        {
            return _fileSystem.GetFileInfo(path);
        }

        #endregion
    }
}
