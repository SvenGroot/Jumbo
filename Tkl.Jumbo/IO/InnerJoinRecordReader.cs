using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Record reader that performs a two-way inner equi-join from two sorted input record readers.
    /// </summary>
    /// <typeparam name="TOuter">The type of the records of the outer relation.</typeparam>
    /// <typeparam name="TInner">The type of the records of the inner relation.</typeparam>
    /// <typeparam name="TResult">The type of the result records.</typeparam>
    /// <remarks>
    /// <para>
    ///   Classes inheriting from <see cref="InnerJoinRecordReader{TOuter, TInner, TResult}"/> must specify
    ///   <see cref="InputTypeAttribute"/> attributes with both <typeparamref name="TOuter"/> and <typeparamref name="TInner"/>.
    /// </para>
    /// </remarks>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1005:AvoidExcessiveParametersOnGenericTypes")]
    public abstract class InnerJoinRecordReader<TOuter, TInner, TResult> : MultiInputRecordReader<TResult>
        where TOuter : class, IWritable, new()
        where TInner : class, IWritable, new()
        where TResult : IWritable, new()
    {
        private RecordReader<TOuter> _outer;
        private RecordReader<TInner> _inner;
        private TOuter _tempOuterObject;
        private readonly List<TInner> _tempInnerList = new List<TInner>();
        private int _tempInnerListIndex;
        private bool _innerHasRecords;
        private bool _outerHasRecords;

        /// <summary>
        /// Initializes a new instance of the <see cref="InnerJoinRecordReader{TOuter, TInner, TResult}"/> class.
        /// </summary>
        /// <param name="partitions">The partitions that this multi input record reader will read.</param>
        /// <param name="totalInputCount">The total number of input readers that this record reader will have.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the record reader may reuse record instances; otherwise, <see langword="false"/>.</param>
        /// <param name="bufferSize">The buffer size to use to read input files.</param>
        /// <param name="compressionType">The compression type to us to read input files.</param>
        protected InnerJoinRecordReader(IEnumerable<int> partitions, int totalInputCount, bool allowRecordReuse, int bufferSize, CompressionType compressionType)
            : base(partitions, totalInputCount, allowRecordReuse, bufferSize, compressionType)
        {
            if( totalInputCount != 2 )
                throw new ArgumentOutOfRangeException("totalInputCount", "InnerJoinRecordReader must have exactly two input readers.");
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        protected sealed override bool ReadRecordInternal()
        {
            if( _outer == null )
            {
                WaitForInputs(2, Timeout.Infinite);
                _outer = (RecordReader<TOuter>)GetInputReader(CurrentPartition, 0);
                _inner = (RecordReader<TInner>)GetInputReader(CurrentPartition, 1);

                _outerHasRecords = _outer.ReadRecord();
                _innerHasRecords = _inner.ReadRecord();
            }

            TOuter outer;

            while( _tempOuterObject == null )
            {
                if( !(_outerHasRecords && _innerHasRecords) )
                {
                    CurrentRecord = default(TResult);
                    return false;
                }

                outer = _outer.CurrentRecord;
                TInner inner = _inner.CurrentRecord;

                int compareResult = Compare(outer, inner);
                if( compareResult < 0 )
                    _outerHasRecords = _outer.ReadRecord();
                else if( compareResult > 0 )
                    _innerHasRecords = _inner.ReadRecord();
                else
                {
                    if( AllowRecordReuse )
                        _tempOuterObject = (TOuter)((ICloneable)outer).Clone();
                    else
                        _tempOuterObject = outer;
                    if( _outerHasRecords = _outer.ReadRecord() )
                    {
                        TOuter nextOuter = _outer.CurrentRecord;
                        if( Compare(nextOuter, inner) == 0 )
                        {
                            // There's more than one record in outer that matches inner, which means we need to store the inner records matching this key
                            // so we can compute the cross product.
                            do
                            {
                                if( AllowRecordReuse )
                                    _tempInnerList.Add((TInner)((ICloneable)inner).Clone());
                                else
                                    _tempInnerList.Add(inner);
                                _innerHasRecords = _inner.ReadRecord();
                                if( _innerHasRecords )
                                    inner = _inner.CurrentRecord;
                            } while( _innerHasRecords && Compare(outer, inner) == 0 );
                        }
                    }
                }
            }

            // We're computing a cross product of an existing matching set of records
            if( !AllowRecordReuse || CurrentRecord == null )
                CurrentRecord = new TResult();
            if( _tempInnerList.Count > 0 )
            {
                CreateJoinResult(CurrentRecord, _tempOuterObject, _tempInnerList[_tempInnerListIndex]);
                ++_tempInnerListIndex;
                if( _tempInnerList.Count == _tempInnerListIndex )
                {
                    _tempInnerListIndex = 0;
                    if( _outerHasRecords && Compare(_outer.CurrentRecord, _tempInnerList[0]) == 0 )
                    {
                        if( AllowRecordReuse )
                            _tempOuterObject = (TOuter)((ICloneable)_outer.CurrentRecord).Clone();
                        else
                            _tempOuterObject = _outer.CurrentRecord;
                        _outerHasRecords = _outer.ReadRecord();
                    }
                    else
                    {
                        _tempOuterObject = null;
                        _tempInnerList.Clear();
                    }
                }
            }
            else
            {
                CreateJoinResult(CurrentRecord, _tempOuterObject, _inner.CurrentRecord);
                _innerHasRecords = _inner.ReadRecord();
                if( !(_innerHasRecords && Compare(_tempOuterObject, _inner.CurrentRecord) == 0) )
                {
                    _tempOuterObject = null;
                }
            }
            return true;
        }

        /// <summary>
        /// When implemented in a derived class, compares an object from the outer relation to one from the inner relation based on the join condition.
        /// </summary>
        /// <param name="outer">The outer relation's object.</param>
        /// <param name="inner">The inner relation's object.</param>
        /// <returns>Less than zero if <paramref name="outer"/> is smaller than the <paramref name="inner"/>; greater than zero if <paramref name="outer"/>
        /// is greater than <paramref name="inner"/>; zero if <paramref name="outer"/> and <paramref name="inner"/> are equal based on the join condition.</returns>
        protected abstract int Compare(TOuter outer, TInner inner);

        /// <summary>
        /// When implemented in a derived class, creates an object of type <typeparamref name="TResult"/> that holds the result of the join.
        /// </summary>
        /// <param name="result">The object that will hold the result.</param>
        /// <param name="outer">The outer relation's object.</param>
        /// <param name="inner">The inner relation's object.</param>
        /// <remarks>
        /// <para>
        ///   If <see cref="MultiInputRecordReader{TRecord}.AllowRecordReuse"/> is <see langword="true"/>, the value of <paramref name="result"/> will be the same every time this function
        ///   is called. It is therefore important that the implementation of this method always sets all relevant properties of the result object.
        /// </para>
        /// </remarks>
        protected abstract void CreateJoinResult(TResult result, TOuter outer, TInner inner);
    }
}
