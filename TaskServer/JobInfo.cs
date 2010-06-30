﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Tkl.Jumbo.Dfs;

namespace TaskServerApplication
{
    sealed class JobInfo
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(JobInfo));

        private readonly Guid _jobId;
        private Dictionary<string, long> _uncompressedTemporaryFileSizes;
        private Dictionary<string, string> _downloadedFiles;

        public JobInfo(Guid jobId)
        {
            _jobId = jobId;
        }

        public void SetUncompressedTemporaryFileSize(string fileName, long uncompressedSize)
        {
            if( _uncompressedTemporaryFileSizes == null )
                _uncompressedTemporaryFileSizes = new Dictionary<string, long>();
            _uncompressedTemporaryFileSizes[fileName] = uncompressedSize;
        }

        public long GetUncompressedTemporaryFileSize(string fileName)
        {
            if( _uncompressedTemporaryFileSizes != null )
            {
                long size;
                if( _uncompressedTemporaryFileSizes.TryGetValue(fileName, out size) )
                    return size;
            }
            return -1L;
        }

        public string DownloadDfsFile(string dfsPath)
        {
            string localPath;
            if( _downloadedFiles == null )
                _downloadedFiles = new Dictionary<string, string>();
            else
            {
                // Did we already download this file?
                if( _downloadedFiles.TryGetValue(dfsPath, out localPath) )
                    return localPath;
            }

            string localJobDirectory = TaskServer.Instance.GetJobDirectory(_jobId);
            string downloadDirectory = Path.Combine(localJobDirectory, "dfs");
            Directory.CreateDirectory(downloadDirectory);

            localPath = Path.Combine(downloadDirectory, "file" + _downloadedFiles.Count.ToString());

            _log.DebugFormat("Downloading DFS file '{0}' to local file '{1}'.", dfsPath, localPath);
            DfsClient client = new DfsClient(TaskServer.Instance.DfsConfiguration);
            client.DownloadFile(dfsPath, localPath);

            _downloadedFiles.Add(dfsPath, localPath);
            return localPath;
        }
    }
}