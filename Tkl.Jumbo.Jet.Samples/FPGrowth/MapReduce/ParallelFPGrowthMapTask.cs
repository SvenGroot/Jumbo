// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet.Samples.FPGrowth.MapReduce
{
    /// <summary>
    /// Map task for PFP Growth
    /// </summary>
    public sealed class ParallelFPGrowthMapTask : Configurable, IPushTask<Utf8String, Pair<int, Transaction>>
    {
        private static readonly char[] _separator = new[] { ' ' };
        private Dictionary<string, int> _itemMapping = new Dictionary<string,int>();
        private List<FGListItem> _fgList;

        /// <summary>
        /// Processes the record.
        /// </summary>
        /// <param name="record">The record.</param>
        /// <param name="output">The output.</param>
        public void ProcessRecord(Utf8String record, RecordWriter<Pair<int, Transaction>> output)
        {
            // Extract the items for the transaction
            string[] items = record.ToString().Split(_separator, StringSplitOptions.RemoveEmptyEntries);
            int itemCount = items.Length;
            // Map them to their item IDs.
            int mappedItemCount = 0;
            int[] mappedItems = new int[itemCount];
            for( int x = 0; x < itemCount; ++x )
            {
                int itemId;
                // Items that are not in the mapping are not frequent.
                if( _itemMapping.TryGetValue(items[x], out itemId) )
                {
                    mappedItems[mappedItemCount] = itemId;
                    ++mappedItemCount;
                }
            }

            if( mappedItemCount > 0 )
            {
                // Sort by item ID; this ensures the items have the same order as they have in the FGList.
                Array.Sort(mappedItems, 0, mappedItemCount);

                int currentGroupId = -1;
                for( int x = 0; x < mappedItemCount; ++x )
                {
                    int groupId = _fgList[mappedItems[x]].GroupId;
                    if( currentGroupId != groupId )
                    {
                        if( currentGroupId != -1 )
                        {
                            OutputGroupTransaction(output, mappedItems, currentGroupId, x);
                        }
                        currentGroupId = groupId;
                    }
                }
                OutputGroupTransaction(output, mappedItems, currentGroupId, mappedItemCount);
            }
        }

        private static void OutputGroupTransaction(RecordWriter<Pair<int, Transaction>> transactionOutput, int[] mappedItems, int currentGroupId, int x)
        {
            int[] groupItems = new int[x];
            Array.Copy(mappedItems, groupItems, x);
            transactionOutput.WriteRecord(Pair.MakePair(currentGroupId, new Transaction() { Items = groupItems, Length = groupItems.Length }));
        }

        /// <summary>
        /// Finishes the task.
        /// </summary>
        /// <param name="output">The output.</param>
        public void Finish(RecordWriter<Pair<int, Transaction>> output)
        {
        }

        /// <summary>
        /// Indicates the configuration has been changed. <see cref="JetActivator.ApplyConfiguration"/> calls this method
        /// after setting the configuration.
        /// </summary>
        public override void NotifyConfigurationChanged()
        {
            _fgList = PFPGrowth.LoadFGList(TaskContext, _itemMapping);
        }
    }
}
