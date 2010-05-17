// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;
using System.IO;

namespace Tkl.Jumbo.Jet.Samples.FPGrowth
{
    /// <summary>
    /// Compact representation of a set of transactions based on the FP-tree structure.
    /// </summary>
    public class TransactionTree : IEnumerable<WeightedTransaction>, IWritable
    {
        #region Nested types

        private struct TreeNode
        {
            public int ItemId;
            public int Count;
            public int ChildCount;
            public int[] Children;

            public void AddChild(int node)
            {
                int newChild = ChildCount++;
                if( Children == null )
                    Children = new int[2];
                else if( Children.Length < ChildCount )
                {
                    int newSize = (int)(Children.Length * _growthRate);
                    Array.Resize(ref Children, newSize);
                }
                Children[newChild] = node;
            }
        }

        private class TreeWalkData
        {
            public int Node;
            public int NextChild;
            public int Sum;
        }

        #endregion

        private TreeNode[] _nodes;
        private int _nodeCount = 1;
        private const float _growthRate = 1.5f;
        private const int _rootNode = 0;

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionTree"/> class.
        /// </summary>
        public TransactionTree()
        {
            _nodes = new TreeNode[8];
            _nodes[_rootNode].ItemId = -1;
        }

        /// <summary>
        /// Adds a transaction.
        /// </summary>
        /// <param name="items">The items of the transaction.</param>
        /// <param name="length">The length.</param>
        public void AddTransaction(IList<int> items, int length)
        {
            int current = _rootNode;
            bool useExistingChild = true;
            for( int x = 0; x < length; ++x )
            {
                int item = items[x];
                int child = useExistingChild ? GetChild(current, item) : 0;
                if( child == 0 )
                {
                    useExistingChild = false; // no need to search for existing children after this.
                    child = CreateNode(current, item, 1);
                }
                else
                    _nodes[child].Count++;

                current = child;
            }
        }

        private int CreateNode(int parentNode, int id, int count)
        {
            if( _nodeCount == _nodes.Length )
                Resize();

            int newNode = _nodeCount++;
            _nodes[newNode].ItemId = id;
            _nodes[newNode].Count = count;
            _nodes[parentNode].AddChild(newNode);

            return newNode;
        }

        private void Resize()
        {
            int newSize = (int)(_nodes.Length * _growthRate);
            Array.Resize(ref _nodes, newSize);
        }

        private int GetChild(int node, int id)
        {
            int childCount = _nodes[node].ChildCount;
            if( childCount > 0 )
            {
                int[] children = _nodes[node].Children;
                for( int x = 0; x < childCount; ++x )
                {
                    int child = children[x];
                    if( _nodes[child].ItemId == id )
                        return child;
                }

            }
            return 0;
        }

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        /// <returns>An enumerator.</returns>
        public IEnumerator<WeightedTransaction> GetEnumerator()
        {
            Stack<TreeWalkData> nodes = new Stack<TreeWalkData>();
            List<int> prefix = new List<int>(); // Not using a stack for this because those iterate top to bottom.
            nodes.Push(new TreeWalkData() { Node = _rootNode });

            while( nodes.Count > 0 )
            {
                TreeWalkData top = nodes.Peek();
                if( top.NextChild < _nodes[top.Node].ChildCount )
                {
                    int child = _nodes[top.Node].Children[top.NextChild];
                    ++top.NextChild;
                    top.Sum += _nodes[child].Count;
                    nodes.Push(new TreeWalkData() { Node = child });
                    prefix.Add(_nodes[child].ItemId);
                }
                else
                {
                    if( top.Node != _rootNode )
                    {
                        int remain = _nodes[top.Node].Count - top.Sum;
                        if( remain > 0 )
                        {
                            yield return new WeightedTransaction() { Count = remain, Items = prefix.ToArray() };
                        }
                        prefix.RemoveAt(prefix.Count - 1);
                    }
                    nodes.Pop();
                }
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
        public void Write(BinaryWriter writer)
        {
            WritableUtility.Write7BitEncodedInt32(writer, _nodeCount);
            for( int x = 0; x < _nodeCount; ++x )
            {
                WritableUtility.Write7BitEncodedInt32(writer, _nodes[x].ItemId);
                WritableUtility.Write7BitEncodedInt32(writer, _nodes[x].Count);
                int childCount = _nodes[x].ChildCount;
                WritableUtility.Write7BitEncodedInt32(writer, childCount);
                for( int child = 0; child < childCount; ++child )
                {
                    WritableUtility.Write7BitEncodedInt32(writer, _nodes[x].Children[child]);
                }
            }
        }

        /// <summary>
        /// Reads the object from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
        public void Read(BinaryReader reader)
        {
            _nodeCount = WritableUtility.Read7BitEncodedInt32(reader);
            if( _nodes == null || _nodes.Length < _nodeCount )
                _nodes = new TreeNode[_nodeCount];
            for( int x = 0; x < _nodeCount; ++x )
            {
                _nodes[x].ItemId = WritableUtility.Read7BitEncodedInt32(reader);
                _nodes[x].Count = WritableUtility.Read7BitEncodedInt32(reader);
                int childCount = WritableUtility.Read7BitEncodedInt32(reader);
                _nodes[x].ChildCount = childCount;
                if( _nodes[x].Children == null || _nodes[x].Children.Length < childCount )
                    _nodes[x].Children = new int[childCount];
                for( int child = 0; child < childCount; ++child )
                {
                    _nodes[x].Children[child] = WritableUtility.Read7BitEncodedInt32(reader);
                }
            }
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return "{ " + this.ToDelimitedString() + " }";
        }
    }
}
