// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo;

namespace Tkl.Jumbo.Jet.Samples.FPGrowth
{
    class FrequentPatternMaxHeap
    {
        private readonly PriorityQueue<MappedFrequentPattern> _queue;
        private readonly int _maxSize;
        private int _minSupport;
        private Dictionary<int, HashSet<MappedFrequentPattern>> _patternIndex;
        private readonly bool _subPatternCheck;
        private int _addCount;

        public FrequentPatternMaxHeap(int maxSize, int minSupport, bool subPatternCheck)
        {
            _minSupport = minSupport;
            _maxSize = maxSize;
            _queue = new PriorityQueue<MappedFrequentPattern>(maxSize + 1, null);
            _subPatternCheck = subPatternCheck;
            if( subPatternCheck )
                _patternIndex = new Dictionary<int, HashSet<MappedFrequentPattern>>();
        }

        public int MinSupport
        {
            get { return _minSupport; }
        }

        public PriorityQueue<MappedFrequentPattern> Queue
        {
            get 
            {
                if( _subPatternCheck )
                {
                    PriorityQueue<MappedFrequentPattern> result = new PriorityQueue<MappedFrequentPattern>(_maxSize, null);
                    foreach( MappedFrequentPattern p in _queue )
                    {
                        if( _patternIndex[p.Support].Contains(p) )
                            result.Enqueue(p);
                    }
                    return result;
                }
                return _queue; 
            }
        }


        public void Add(MappedFrequentPattern pattern)
        {
            if( _queue.Count == _maxSize )
            {
                if( pattern.CompareTo(_queue.Peek()) > 0 && AddInternal(pattern) )
                {
                    MappedFrequentPattern removedPattern = _queue.Dequeue();
                    if( _subPatternCheck )
                        _patternIndex[removedPattern.Support].Remove(removedPattern);
                    _minSupport = _queue.Peek().Support;
                }
            }
            else
            {
                if( AddInternal(pattern) )
                {
                    _minSupport = Math.Min(_minSupport, pattern.Support);
                }
            }
        }

        private bool AddInternal(MappedFrequentPattern pattern)
        {
            ++_addCount;
            if( !_subPatternCheck )
            {
                _queue.Enqueue(pattern);
                return true;
            }
            else
            {
                HashSet<MappedFrequentPattern> index;
                if( _patternIndex.TryGetValue(pattern.Support, out index) )
                {
                    MappedFrequentPattern patternToReplace = null;
                    foreach( MappedFrequentPattern p in index )
                    {
                        if( pattern.IsSubpatternOf(p) )
                            return false;
                        else if( p.IsSubpatternOf(pattern) )
                        {
                            patternToReplace = p;
                            break;
                        }
                    }

                    if( patternToReplace != null )
                    {
                        index.Remove(patternToReplace);
                        _queue.Remove(patternToReplace);
                        if( !index.Contains(pattern) )
                        {
                            _queue.Enqueue(pattern);
                            index.Add(pattern);
                        }
                        return false;
                    }
                }
                else
                {
                    index = new HashSet<MappedFrequentPattern>();
                    _patternIndex.Add(pattern.Support, index);
                }

                _queue.Enqueue(pattern);
                index.Add(pattern);
                return true;
            }
        }

    }
}
