# zfssnap
zfssnap is a python script that automates the task of creating snapshots
for ZFS on Linux systems. By default it will snapshot all ZFS filesystems on
the host, but this can be overriden either globally per file system or per 
label via ZFS properties.
The same goes for `keep` values which also can be overriden per
label. The properties are subject to the same inheritance rules as other
ZFS properties.

zfssnap doesn't really care if the file system is local or accessed via SSH,
so it may also be used to handle snapshots and replication on remote servers
without installing zfssnap on the remote servers, but especially replication 
performance may suffer.

As ZFS properties are used to identify the snapshot label the snapshot names
are compatible with the shadow_copy2 module in Samba for use with
Previous Versions.

## Requirements
* Only tested on Python v3.4 on Debian Jessie.

## ZFS properties used
* `zol:zfssnap:label=[str]`: Identifies the label of the snapshot.
* `zol:zfssnap=[on|off]`: Toggle snapshots for all labels on a file
  system. If not set snapshots are enabled unless disabled per label.
* `zol:zfssnap:<label>=[on|off]`: Toggle snapshots for a specific label.
  Overrides the global property. If not set snapshots are enabled unless
  globally disabled for the file system.
* `zol:zfssnap:keep=[int]`: Override the `keep` value for a file system.
  This overrides `--keep` given on the command line for that file system.
* `zol:zfssnap:<label>:keep=[int]`: Override the `keep` value for a label.
  This overrides the global property and the value given on the command line.
* By using `--default-exclude` with the snapshot command you invert the logic
  and must explicitly enable snapshots using the ZFS properties. 
* `--override` will always enable snapshots and use the given `--keep` value
  regardless of the ZFS properties.

## Usage
Create a snapshot of all ZFS file systems labeled `hourly`. Keep no more than 24
snapshots by deleting the oldest ones:

    ./zfssnap.py snapshot --label hourly --keep 24
...which is the same as:

    ./zfssnap.py snapshot --label hourly --keep 24 --file-systems _all
Or alternatively if you want to do the same on some remote server:

    ./zfssnap.py snapshot --label hourly --keep 24 --file-systems user@remoteserver:_all
Give a list of file systems to snapshot:

    ./zfssnap.py snapshot --label hourly --keep 24 --file-systems user@remoteserver:prod/vms zpool/files root@server2:pool/stuff
Delete all snapshots for a label on a selected file system:

    ./zfssnap.py snapshot --label monthly --keep 0 --file-systems zpool1/dev
Replicate local file system to another local dataset:

    ./zfssnap.py replicate --label replicated --keep 1 --src-file-system pool1/dataset1 --dst-file-system pool2/dataset2
Replicate a local file system to remote dataset using ssh:

    ./zfssnap.py replicate --label replicated --keep 1 --src-file-system pool1/dataset1 --dst-file-system user@remoteserver:pool2/dataset2
Replicate remote file system to a local dataset:

    ./zfssnap.py replicate --label replicated --keep 1 --src-file-system user@remoteserver:pool1/dataset1 --dst-file-system pool2/dataset2
You may even replicate a remote file system to a remote dataset if you want.
In other words you may use zfssnap to remotely handle snapshots and replication
on other ZFS servers.

The following syntaxes are valid when specifiying a file system:

    1. ssh_user@ssh_server:filesystem
    2. filesystem

You need to take care of the key distribution for passwordless logins by normal
ssh mechanisms before the ssh options can be used.

List all options

    ./zfssnap.py --help
Get help for sub-command

    ./zfssnap.py replicate --help

## Schedule snapshots
To schedule snapshots crontab are normally used. This is an example root
crontab for this purpose:

    */15 *      *  *  *   /usr/sbin/zfssnap --verbosity WARNING snapshot --label frequent --keep 4
    8    */1    *  *  *   /usr/sbin/zfssnap --verbosity WARNING snapshot --label hourly --keep 24
    16   0      *  *  *   /usr/sbin/zfssnap --verbosity WARNING snapshot --label daily --keep 31
    */5  *      *  *  *   /usr/sbin/zfssnap --verbosity WARNING replicate --label replicated --keep 1 --src-file-system prod/vms --dst-file-system root@backupserver:backup/vms

* `zfssnap.py` has been symlinked to `/usr/sbin/zfssnap` for ease of use.
* `--quiet` can be used to supress all output, even warnings and errors.
  However, you are normally interested in getting a notification from cron if 
  something goes wrong.

## Samba configuration for Previous Version
The .zfs directory can remain hidden.

    [global]
    shadow: snapdir = .zfs/snapshot
    shadow: sort = desc
    shadow: format = zfssnap_%Y%m%dT%H%M%SZ
    shadow: localtime = no

    [<some share>]
    vfs_objects = shadow_copy2

## Example usage of ZFS properties
List snapshots with zfssnap labels

    zfs list -o name,zol:zfssnap:label -t snapshot
Disable snapshots for a label on a dataset

    zfs set zol:zfssnap:monthly=false zpool1/temp
Override `keep` value for label on dataset

    zfs set zol:zfssnap:daily:keep=62 zpool1/www
Remove property from dataset properties

    zfs inherit zol:zfssnap:daily:keep zpool1/temp
