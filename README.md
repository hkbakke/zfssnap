# zfs-snap
zfs-snap is a python script that automates the task of creating snapshots
for ZFS on Linux systems. By default it will snapshot all ZFS filesystems on
the host, but this can be overriden either globally or per label via ZFS
properties. The same goes for `keep` values which also can be overriden per
label. The properties are subject to the same inheritance rules as other
ZFS properties.

As ZFS properties are used to identify the snapshot label the snapshot names
are compatible with the shadow_copy2 module in Samba for use with
Previous Versions.

## Requirements
* Only tested on Python v3.4 on Debian Jessie.

## ZFS properties used
* `zol:zfs-snap:label=[str]`: Identifies the label of the snapshot.
* `zol:zfs-snap=[true|false|-]`: Toggle snapshots for all labels on a file
  system. Equals `true` if not set.
* `zol:zfs-snap:<label>=[true|false|-]`: Toggle snapshots for a specific label.
  Equals `true` if not set. Overrides the global property.
* `zol:zfs-snap:keep=[int]`: Override the `keep` value for a file system.
  This overrides the keep given on the command line for that file system.
  May be overridden by command line by specifying the `--force` option.
* `zol:zfs-snap:<label>:keep=[int]`: Override the `keep` value for a label.
  This overrides the global property and the value given on the command line.
  May be overridden by command line by specifying the `--force` option.

## Usage
Create a snapshot of all ZFS file systems labeled `hourly`. Keep no more than 24
snapshots by deleting the oldest ones.

    ./zfs-snap.py --label=hourly --keep=24
Delete all snapshots for a label on all file systems. Note that disabling
snapshots for a file system using properties will automatically delete all
existing snapshots on the next run for that label or file system.

    ./zfs-snap.py --label=monthly --keep=0
Override `keep` value set in ZFS property

    ./zfs-snap.py --label=frequent --keep=4 --force
List all options:
    ./zfs-snap.py --help

## Scedule snapshots
To schedule snapshots crontab are normally used. This is an example root
crontab for this purpose:

    */15 *      *  *  *   /usr/sbin/zfs-snap --label=frequent --keep=4 -q
    8    */1    *  *  *   /usr/sbin/zfs-snap --label=hourly --keep=24 -q
    16   0      *  *  *   /usr/sbin/zfs-snap --label=daily --keep=31 -q

* `zfs-snap.py` have been symlinked to `/usr/sbin/zfs-snap` for ease of use.
* Make sure the snapshot jobs are not triggered at exactly the same time 
  (normally by using the same minute). The time resolution of the snapshot 
  naming are 1 second, but you may still have name collisions when the cron 
  jobs are triggered at the same time, as the label are not included in the 
  snapshot name to be compatible with Previous Versions. 
  Nothing bad happens, though. The script just exits with an error and you get
  no snapshots that run.

## Samba configuration for Previous Version
The .zfs directory can remain hidden.

    [global]
    shadow: snapdir = .zfs/snapshot
    shadow: sort = desc
    shadow: format = zfs-snap_%Y%m%dT%H%M%SZ
    shadow: localtime = no

    [<some share>]
    vfs_objects = shadow_copy2

## Example usage of ZFS properties
List snapshots with zfs-snap labels

    zfs list -o name,zol:zfs-snap:label -t snapshot
Disable snapshots for a label on a dataset

    zfs set zol:zfs-snap:monthly=false zpool1/temp
Override `keep` value for label on dataset

    zfs set zol:zfs-snap:daily:keep=62 zpool1/www