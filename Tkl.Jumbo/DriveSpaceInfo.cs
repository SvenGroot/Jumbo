using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides information about the free disk space of a drive.
    /// </summary>
    public class DriveSpaceInfo
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DriveSpaceInfo"/> class.
        /// </summary>
        /// <param name="drivePath">The path of the drive for which to get the amount of space.</param>
        public DriveSpaceInfo(string drivePath)
        {
            if( drivePath == null )
                throw new ArgumentNullException("drivePath");

            if( RuntimeEnvironment.RuntimeType == RuntimeEnvironmentType.Mono )
                GetDriveSpaceMono(drivePath);
            else
                GetDriveSpace(drivePath);
        }

        /// <summary>
        /// Gets the amount of available free space.
        /// </summary>
        public long AvailableFreeSpace { get; private set; }
        /// <summary>
        /// Gets the total amount of free space.
        /// </summary>
        public long TotalFreeSpace { get; private set; }
        /// <summary>
        /// Gets the total size of the drive.
        /// </summary>
        public long TotalSize { get; private set; }

        private void GetDriveSpace(string drivePath)
        {
            DriveInfo drive = new DriveInfo(drivePath);
            AvailableFreeSpace = drive.AvailableFreeSpace;
            TotalFreeSpace = drive.TotalFreeSpace;
            TotalSize = drive.TotalSize;
        }

        private void GetDriveSpaceMono(string drivePath)
        {
            Type unixDriveInfo = Type.GetType("Mono.Unix.UnixDriveInfo, Mono.Posix, Version=2.0.0.0, Culture=neutral, PublicKeyToken=0738eb9f132ed756");
            if( unixDriveInfo != null )
            {
                object drive = Activator.CreateInstance(unixDriveInfo, drivePath);
                AvailableFreeSpace = (long)unixDriveInfo.GetProperty("AvailableFreeSpace").GetValue(drive, null);
                TotalFreeSpace = (long)unixDriveInfo.GetProperty("TotalFreeSpace").GetValue(drive, null);
                TotalSize = (long)unixDriveInfo.GetProperty("TotalSize").GetValue(drive, null);
            }
        }
    }
}
