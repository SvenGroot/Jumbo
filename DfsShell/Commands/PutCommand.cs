﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.CommandLine;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.IO;
using Tkl.Jumbo.Dfs;
using Tkl.Jumbo;
using Tkl.Jumbo.IO;

namespace DfsShell.Commands
{
    [ShellCommand("put"), Description("Stores a file or directory on the DFS.")]
    class PutCommand : DfsShellCommandWithProgress
    {
        private readonly string _localPath;
        private readonly string _dfsPath;

        public PutCommand([Description("The path of the local file or directory to upload.")] string localPath,
                              [Description("The path of the DFS file or directory to upload to.")] string dfsPath)
        {
            if( localPath == null )
                throw new ArgumentNullException("localPath");
            if( dfsPath == null )
                throw new ArgumentNullException("dfsPath");

            _localPath = localPath;
            _dfsPath = dfsPath;
        }

        [NamedCommandLineArgument("b"), Description("The block size of the DFS file.")]
        public ByteSize BlockSize { get; set; }

        [NamedCommandLineArgument("r"), Description("The replication factor of the DFS file.")]
        public int ReplicationFactor { get; set; }

        [NamedCommandLineArgument("q"), Description("Suppress progress information output.")]
        public bool Quiet { get; set; }

        [NamedCommandLineArgument("rr"), Description("The record reader used to read the file(s). This must be the assembly-qualified name of the type. If this argument is specified, you must also specify a record writer using the same record type.")]
        public string RecordReaderTypeName { get; set; }

        [NamedCommandLineArgument("rw"), Description("The record writer used to write the file(s) to the DFS. This must be the assembly-qualified name of the type. If this argument is specified, you must also specify a record writer using the same record type.")]
        public string RecordWriterTypeName { get; set; }

        [NamedCommandLineArgument("ro"), Description("The record options for the file. Must be a comma-separated list of the values of the RecordStreamOptions enumeration. If this option is anything other than None, you must specify a record reader and record writer.")]
        public RecordStreamOptions RecordOptions { get; set; }

        [NamedCommandLineArgument("text"), Description("Treat the file as line-separated text. This is equivalent to specifying LineRecordReader as the record reader and TextRecordReader<Utf8String> as the record writer.")]
        public bool TextFile { get; set; }

        public override void Run()
        {
            Type recordReaderType;
            Type recordWriterType;
            if( !File.Exists(_localPath) && !Directory.Exists(_localPath) )
                Console.Error.WriteLine("Local path {0} does not exist.", _localPath);
            else if( BlockSize.Value < 0 || BlockSize.Value >= Int32.MaxValue )
                Console.Error.WriteLine("Invalid block size.");
            else if( CheckRecordOptions(out recordReaderType, out recordWriterType) )
            {                
                ProgressCallback progressCallback = Quiet ? null : new ProgressCallback(PrintProgress);
                try
                {
                    bool isDirectory = Directory.Exists(_localPath);
                    if( isDirectory )
                    {
                        if( !Quiet )
                            Console.WriteLine("Copying local directory \"{0}\" to DFS directory \"{1}\"...", _localPath, _dfsPath);
                        if( recordReaderType != null )
                            UploadDirectoryRecords(_localPath, _dfsPath, recordReaderType, recordWriterType);
                        else
                            Client.UploadDirectory(_localPath, _dfsPath, (int)BlockSize.Value, ReplicationFactor, progressCallback);
                    }
                    else
                    {
                        DfsDirectory dir = Client.NameServer.GetDirectoryInfo(_dfsPath);
                        string dfsPath = _dfsPath;
                        if( dir != null )
                        {
                            string fileName = Path.GetFileName(_localPath);
                            dfsPath = DfsPath.Combine(dfsPath, fileName);
                        }
                        if( !Quiet )
                            Console.WriteLine("Copying local file \"{0}\" to DFS file \"{1}\"...", _localPath, dfsPath);
                        if( recordReaderType != null )
                            UploadFileRecords(_localPath, dfsPath, recordReaderType, recordWriterType);
                        else
                            Client.UploadFile(_localPath, dfsPath, (int)BlockSize.Value, ReplicationFactor, progressCallback);
                    }
                    if( !Quiet )
                        Console.WriteLine();
                }
                catch( UnauthorizedAccessException ex )
                {
                    Console.Error.WriteLine("Unable to open local file:");
                    Console.Error.WriteLine(ex.Message);
                }
                catch( IOException ex )
                {
                    Console.Error.WriteLine("Unable to read local file:");
                    Console.Error.WriteLine(ex.Message);
                }
            }
        }

        private void UploadFileRecords(string localPath, string dfsPath, Type recordReaderType, Type recordWriterType)
        {
            int previousPercentage = -1;
            using( FileStream inputStream = File.OpenRead(localPath) )
            using( IRecordReader reader = (IRecordReader)Activator.CreateInstance(recordReaderType, inputStream) )
            using( DfsOutputStream outputStream = Client.CreateFile(dfsPath, (int)BlockSize.Value, ReplicationFactor, RecordOptions) )
            using( IRecordWriter writer = (IRecordWriter)Activator.CreateInstance(recordWriterType, outputStream) )
            {
                while( reader.ReadRecord() )
                {
                    writer.WriteRecord(reader.CurrentRecord);
                    if( !Quiet )
                    {
                        int percentage = (int)(reader.Progress * 100);
                        if( percentage != previousPercentage )
                        {
                            previousPercentage = percentage;
                            PrintProgress(dfsPath, percentage, inputStream.Position);
                        }
                    }
                }
            }
        }

        private void UploadDirectoryRecords(string localPath, string dfsPath, Type recordReaderType, Type recordWriterType)
        {
            string[] files = System.IO.Directory.GetFiles(localPath);

            DfsDirectory directory = Client.NameServer.GetDirectoryInfo(dfsPath);
            if( directory != null )
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Directory {0} already exists on the DFS.", dfsPath), "dfsPath");
            Client.NameServer.CreateDirectory(dfsPath);

            foreach( string file in files )
            {
                string targetFile = DfsPath.Combine(dfsPath, System.IO.Path.GetFileName(file));
                UploadFileRecords(file, targetFile, recordReaderType, recordWriterType);
            }
        }

        private bool CheckRecordOptions(out Type recordReaderType, out Type recordWriterType)
        {
            recordReaderType = null;
            recordWriterType = null;
            if( TextFile )
            {
                if( !(RecordReaderTypeName == null && RecordWriterTypeName == null) )
                {
                    Console.Error.WriteLine("You may not specify a record reader or record writer if the -text option is specified.");
                    return false;
                }
                recordReaderType = typeof(LineRecordReader);
                recordWriterType = typeof(TextRecordWriter<Utf8String>);
                return true;
            }
            else if( RecordReaderTypeName != null || RecordWriterTypeName != null )
            {
                if( RecordReaderTypeName == null || RecordWriterTypeName == null )
                {
                    Console.Error.WriteLine("You must specify both a record reader and a record writer.");
                    return false;
                }
                recordReaderType = Type.GetType(RecordReaderTypeName, true);
                recordWriterType = Type.GetType(RecordWriterTypeName, true);

                Type recordReaderRecordType = recordReaderType.FindGenericBaseType(typeof(RecordReader<>), true).GetGenericArguments()[0];
                Type recordWriterRecordType = recordWriterType.FindGenericBaseType(typeof(RecordWriter<>), true).GetGenericArguments()[0];
                if( recordReaderRecordType != recordWriterRecordType )
                {
                    Console.Error.WriteLine("The record reader and writer must have the same record types.");
                    return false;
                }

                return true;
            }
            else if( RecordOptions != RecordStreamOptions.None )
            {
                Console.Error.WriteLine("You must specify a record reader and writer if the -ro option is set to anything other than None.");
                return false;
            }

            return true;
        }
    }
}
