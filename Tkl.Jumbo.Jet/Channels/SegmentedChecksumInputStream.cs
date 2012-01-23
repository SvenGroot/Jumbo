using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo.Jet.Channels
{
    class SegmentedChecksumInputStream : Stream
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(SegmentedChecksumInputStream));

        private readonly Stream _baseStream;
        private readonly long _length;
        private readonly string _fileName;
        private readonly bool _deleteFile;
        private readonly byte[] _sizeBuffer = new byte[sizeof(long)];
        private ChecksumInputStream _currentSegment;
        private long _position;
        private bool _disposed;

        public SegmentedChecksumInputStream(string fileName, int bufferSize, bool deleteFile, int segmentCount)
            : this(new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize), segmentCount)
        {
            _fileName = fileName;
            _deleteFile = deleteFile;
        }

        public SegmentedChecksumInputStream(Stream baseStream, int segmentCount)
        {
            if( baseStream == null )
                throw new ArgumentNullException("baseStream");
            if( segmentCount < 1 )
                throw new ArgumentOutOfRangeException("segmentCount");
            _baseStream = baseStream;
            _length = _baseStream.Length - (segmentCount * (sizeof(long) + 1));
            NextSegment();

            // We assume that if the checksum is enabled for one segment, it's enabled for all.
            if( _currentSegment.IsChecksumEnabled )
                _length -= (sizeof(uint) * segmentCount);
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
            if( _disposed )
                throw new ObjectDisposedException(typeof(SegmentedChecksumInputStream).FullName);

            if( count < 0 )
                throw new ArgumentOutOfRangeException("count");

            int totalBytesRead = 0;
            while( count > 0 )
            {
                int bytesRead = _currentSegment.Read(buffer, offset, count);
                if( bytesRead == 0 && !NextSegment() )
                    break;
                count -= bytesRead;
                offset += bytesRead;
                _position += bytesRead;
                totalBytesRead += bytesRead;
            }

            return totalBytesRead;
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
            try
            {
                if( !_disposed )
                {
                    _disposed = true;
                    if( _currentSegment != null )
                    {
                        _currentSegment.Dispose();
                        _currentSegment = null;
                    }
                    _baseStream.Dispose();
                    if( _deleteFile )
                    {
                        try
                        {
                            if( File.Exists(_fileName) )
                            {
                                File.Delete(_fileName);
                            }
                        }
                        catch( IOException ex )
                        {
                            _log.Error(string.Format(System.Globalization.CultureInfo.InvariantCulture, "Failed to delete file {0}.", _fileName), ex);
                        }
                        catch( UnauthorizedAccessException ex )
                        {
                            _log.Error(string.Format(System.Globalization.CultureInfo.InvariantCulture, "Failed to delete file {0}.", _fileName), ex);
                        }
                    }
                }
            }
            finally
            {
                base.Dispose(disposing);
            }
        }

        private bool NextSegment()
        {
            if( _currentSegment != null )
                _currentSegment.Dispose();

            if( _position < _length )
            {
                int bytesRead = _baseStream.Read(_sizeBuffer, 0, _sizeBuffer.Length);
                if( bytesRead < _sizeBuffer.Length )
                    throw new IOException("Invalid segmented stream.");

                long segmentLength = BitConverter.ToInt64(_sizeBuffer, 0);
                _currentSegment = new ChecksumInputStream(_baseStream, false, segmentLength);
                return true;
            }
            else
                return false;
        }
    }
}
