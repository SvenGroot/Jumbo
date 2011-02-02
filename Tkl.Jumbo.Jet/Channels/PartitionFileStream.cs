// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;

namespace Tkl.Jumbo.Jet.Channels
{
    sealed class PartitionFileStream : Stream
    {
        private Stream _baseStream;
        private readonly string _fileName;
        private readonly IEnumerator<PartitionFileIndexEntry> _indexEntries;
        private PartitionFileIndexEntry _current;
        private long _segmentPosition;
        private readonly long _length;
        private long _position;
        private readonly int _bufferSize;

        public PartitionFileStream(string fileName, int bufferSize, IEnumerable<PartitionFileIndexEntry> indexEntries)
        {
            _fileName = fileName;
            _bufferSize = bufferSize;
            _length = indexEntries.Sum(e => e.Count);
            _indexEntries = indexEntries.GetEnumerator();
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
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 )
                throw new ArgumentOutOfRangeException("offset");
            if( count < 0 )
                throw new ArgumentOutOfRangeException("count");
            if( offset + count > buffer.Length )
                throw new ArgumentException("The sum of offset and count is greater than the buffer length.");

            if( _baseStream == null )
                _baseStream = new FileStream(_fileName, FileMode.Open, FileAccess.Read, FileShare.Read, _bufferSize);

            if( _current.Count == 0 || _segmentPosition == _current.Count )
            {
                if( !_indexEntries.MoveNext() )
                    return 0;
                _current = _indexEntries.Current;
                _baseStream.Seek(_current.Offset, SeekOrigin.Begin);
                _segmentPosition = 0;
            }

            int bytesRead;
            if( _segmentPosition + count > _current.Count )
            {
                int firstCount = (int)(_current.Count - _segmentPosition);
                bytesRead = _baseStream.Read(buffer, offset, firstCount);
                if( _indexEntries.MoveNext() )
                {
                    _current = _indexEntries.Current;
                    _baseStream.Seek(_current.Offset, SeekOrigin.Begin);
                    int bytesRead2 = _baseStream.Read(buffer, offset + firstCount, count - firstCount);
                    bytesRead += bytesRead2;
                    _segmentPosition = bytesRead2;
                }
                else
                {
                    _segmentPosition = 0;
                    _current = default(PartitionFileIndexEntry);
                }
            }
            else
            {
                bytesRead = _baseStream.Read(buffer, offset, count);
                _segmentPosition += bytesRead;
            }

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

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if( disposing )
            {
                _indexEntries.Dispose();
                if( _baseStream != null )
                _baseStream.Dispose();
            }
        }
    }
}
