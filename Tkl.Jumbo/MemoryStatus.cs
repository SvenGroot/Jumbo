using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Management;
using System.Runtime.InteropServices;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Represents a snapshot of the system's memory status.
    /// </summary>
    public sealed class MemoryStatus
    {
        private long _totalPhysicalMemory;
        private long _availablePhysicalMemory;
        private long _cachedMemory;
        private long _bufferedMemory;
        private long _totalSwap;
        private long _availableSwap;

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryStatus"/> class.
        /// </summary>
        public MemoryStatus()
        {
            Refresh();
        }

        /// <summary>
        /// Gets the total size of the physical memory.
        /// </summary>
        public long TotalPhysicalMemory
        {
            get { return _totalPhysicalMemory; }
        }

        /// <summary>
        /// Gets the amount of available physical memory.
        /// </summary>
        /// <remarks>
        /// This is not the same as free physical memory. Cached memory is considered available.
        /// </remarks>
        public long AvailablePhysicalMemory
        {
            get { return _availablePhysicalMemory; }
        }

        /// <summary>
        /// Gets the amount of memory that is used as cache. This value is not available on Windows.
        /// </summary>
        public long CachedMemory
        {
            get { return _cachedMemory; }
        }

        /// <summary>
        /// Gets the amount of memory that is used as buffers. This value is not available on Windows.
        /// </summary>
        public long BufferedMemory
        {
            get { return _bufferedMemory; }
        }

        /// <summary>
        /// Gets the total size of the swap space.
        /// </summary>
        public long TotalSwap
        {
            get { return _totalSwap; }
        }

        /// <summary>
        /// Gets the amount of swap space available.
        /// </summary>
        public long AvailableSwap
        {
            get { return _availableSwap; }
        }

        /// <summary>
        /// Refreshes the memory snapshot.
        /// </summary>
        public void Refresh()
        {
            if( Environment.OSVersion.Platform == PlatformID.Win32NT )
                RefreshWindows();
            else if( Environment.OSVersion.Platform == PlatformID.Unix )
                RefreshUnix();
        }

        /// <summary>
        /// Returns a string representation of the current <see cref="MemoryStatus"/>.
        /// </summary>
        /// <returns>A string representation of the current <see cref="MemoryStatus"/>.</returns>
        public override string ToString()
        {
            if( Environment.OSVersion.Platform == PlatformID.Win32NT )
                return string.Format(System.Globalization.CultureInfo.CurrentCulture, "Physical: {0}M total, {1}M available; Page file: {2}M total, {3}M available.", TotalPhysicalMemory / ByteSize.Megabyte, AvailablePhysicalMemory / ByteSize.Megabyte, TotalSwap / ByteSize.Megabyte, AvailableSwap / ByteSize.Megabyte);
            else if( Environment.OSVersion.Platform == PlatformID.Unix )
                return string.Format(System.Globalization.CultureInfo.CurrentCulture, "Physical: {0}M total, {1}M available, {2}M buffered, {3}M cached; Swap: {4}M total, {5}M available.", TotalPhysicalMemory / ByteSize.Megabyte, AvailablePhysicalMemory / ByteSize.Megabyte, BufferedMemory / ByteSize.Megabyte, CachedMemory / ByteSize.Megabyte, TotalSwap / ByteSize.Megabyte, AvailableSwap / ByteSize.Megabyte);
            else
                return "No memory information.";
        }

        private void RefreshWindows()
        {
            NativeMethods.PERFORMANCE_INFORMATION performanceInfo;
            if( !NativeMethods.GetPerformanceInfo(out performanceInfo, Marshal.SizeOf(typeof(NativeMethods.PERFORMANCE_INFORMATION))) )
                throw new System.ComponentModel.Win32Exception();

            _totalPhysicalMemory = (long)performanceInfo.PhysicalTotal * (long)performanceInfo.PageSize;
            _availablePhysicalMemory = (long)performanceInfo.PhysicalAvailable * (long)performanceInfo.PageSize;

            SelectQuery query = new SelectQuery("Win32_PageFileUsage", null, new[] { "CurrentUsage", "AllocatedBaseSize" });
            ManagementObjectSearcher searcher = new ManagementObjectSearcher(query);

            _totalSwap = 0;
            _availableSwap = 0;
            foreach( ManagementBaseObject obj in searcher.Get() )
            {
                long size = (uint)obj["AllocatedBaseSize"] * ByteSize.Megabyte;
                long used = (uint)obj["CurrentUsage"] * ByteSize.Megabyte;
                _totalSwap += size;
                _availableSwap += (size - used);
            }
        }

        private void RefreshUnix()
        {
            if( File.Exists("/proc/meminfo") )
            {
                int neededFields = 6;
                using( StreamReader reader = File.OpenText("/proc/meminfo") )
                {
                    string line;
                    while( neededFields > 0 && (line = reader.ReadLine()) != null )
                    {
                        if( ExtractMemInfoValue(line, "MemTotal:", ref _totalPhysicalMemory) ||
                            ExtractMemInfoValue(line, "MemFree:", ref _availablePhysicalMemory) ||
                            ExtractMemInfoValue(line, "Buffers:", ref _bufferedMemory) ||
                            ExtractMemInfoValue(line, "Cached:", ref _cachedMemory) ||
                            ExtractMemInfoValue(line, "SwapTotal:", ref _totalSwap) ||
                            ExtractMemInfoValue(line, "SwapFree:", ref _availableSwap) )
                            --neededFields;
                    }
                }

                // Correct for the difference between free and available.
                _availablePhysicalMemory += _cachedMemory;
            }
        }

        private bool ExtractMemInfoValue(string line, string field, ref long value)
        {
            if( line.StartsWith(field, StringComparison.Ordinal) )
            {
                // Strip the field, the colon, and the kB
                string valueString = line.Substring(field.Length + 1, line.Length - field.Length - 3);
                value = long.Parse(valueString, System.Globalization.CultureInfo.InvariantCulture) * ByteSize.Kilobyte;
                return true;
            }
            else
                return false;
        }
    }
}
