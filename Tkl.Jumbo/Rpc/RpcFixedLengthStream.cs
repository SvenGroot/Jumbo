using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.Rpc
{
    class RpcFixedLengthStream : Stream
    {
        private readonly Stream _baseStream;
        private long _length;
        private long _position;

        public RpcFixedLengthStream(Stream baseStream, long length)
        {
            _baseStream = baseStream;
            _length = length;
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
        }

        public override long Length
        {
            get { return _length; }
        }

        public override long Position
        {
            get
            {
                return _position;
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            long remainingLength = _length - _position;
            int realCount = (int)Math.Min(remainingLength, count);
            if( realCount == 0 )
                return 0;
            int bytesRead = _baseStream.Read(buffer, offset, realCount);
            _position += bytesRead;
            return bytesRead;
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
}
