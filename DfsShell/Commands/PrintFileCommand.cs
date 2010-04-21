using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.CommandLine;
using System.ComponentModel;
using Tkl.Jumbo.Dfs;
using System.IO;

namespace DfsShell.Commands
{
    [ShellCommand("cat"), Description("Prints a text file.")]
    class PrintFileCommand : DfsShellCommand
    {
        #region Nested types

        private class SizeLimitedStream : Stream
        {
            private readonly Stream _baseStream;
            private readonly long _size;

            public SizeLimitedStream(Stream baseStream, long size)
            {
                _baseStream = baseStream;
                _size = size;
            }

            public override bool CanRead
            {
                get { return true; }
            }

            public override bool CanSeek
            {
                get { return false; }
            }

            public override bool CanWrite
            {
                get { return false; }
            }

            public override void Flush()
            {
                _baseStream.Flush();
            }

            public override long Length
            {
                get { return Math.Min(_size, _baseStream.Length); }
            }

            public override long Position
            {
                get
                {
                    return _baseStream.Position;
                }
                set
                {
                    throw new NotSupportedException();
                }
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                if( Position + count > _size )
                {
                    count = (int)(_size - Position);
                    if( count < 0 )
                        count = 0;
                }
                

                return _baseStream.Read(buffer, offset, count);
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }
        }


        #endregion

        private readonly string _path;

        public PrintFileCommand([Description("The path of the text file on the DFS.")] string path)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            _path = path;
        }

        [NamedCommandLineArgument("encoding", DefaultValue="utf-8"), Description("The text encoding to use. The default value is utf-8.")]
        public string Encoding { get; set; }

        [NamedCommandLineArgument("size", DefaultValue=long.MaxValue), Description("The maximum number of bytes to read from the file. If not specified, the entire file will be read.")]
        public long Size { get; set; }

        [NamedCommandLineArgument("tail"), Description("Prints the end rather than the start of the file up to the specified size.")]
        public bool Tail { get; set; }

        public override void Run()
        {
            Encoding encoding = System.Text.Encoding.GetEncoding(Encoding);

            using( DfsInputStream stream = Client.OpenFile(_path) )
            {
                if( Tail )
                {
                    long newPosition = stream.Length - Size;
                    if( newPosition > 0 )
                        stream.Position = newPosition;
                }
                using( SizeLimitedStream limitedStream = new SizeLimitedStream(stream, Tail ? long.MaxValue : Size) )
                using( StreamReader reader = new StreamReader(limitedStream, encoding) )
                {
                    string line;
                    while( (line = reader.ReadLine()) != null )
                        Console.WriteLine(line);
                }
            }
        }
    }
}
