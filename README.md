# zfssnap
zfssnap is a python script that automates the task of creating snapshots and/or
replicating snapshots on ZFS on Linux systems.

As ZFS properties are used to identify the snapshot label the snapshot names
are compatible with the shadow_copy2 module in Samba for use with
Previous Versions.

It is made for the following purposes:
* Manage automatic snapshoting
* Manage dataset replication between network connected hosts
* Manage file based replication between one-way connected or disconnected hosts

## Requirements
* Tested on Debian Jessie and Stretch
* ZFS on Linux packages
* Python >= 3.4
* Python modules: yaml, scandir (Python < v3.5 only)

## ZFS properties used
* `zfssnap:label=[str]`: Identifies the label of the snapshot.
* `zfssnap:repl_status=[str]`: Used to keep replication state of a snapshot
* `zfssnap:version=[str]`: zfssnap version used to create the snapshot

## Configuration
zfssnap expects the configuration file to be located at
`/etc/zfssnap/zfssnap.yml`.
You can override this location using the `--config` argument.

In versions before v3.0.0 zfssnap stored its configuration in ZFS properties and
had many more command line arguments, but this proved confusing, inflexible and
unmanagable in more complex setups. ZFS properties are now only used for keeping
state and information about the snapshot, while the configuration is stored in
the configuration file.

## Usage
zfssnap works with the concept of policies. These are defined in the YAML based
configuration file. An example file is provided with the source.
Running the command normally just involves pointing to a policy name.

Create a snapshot of all ZFS file systems defined in the policy `snapshot-all`.

    ./zfssnap.py --policy snapshot-all
Remove all snapshots for a policy or re-initialize a replication policy

    ./zfssnap.py --policy snapshot-all --reset

List all snapshots belonging to a policy

    ./zfssnap.py --policy snapshot-all --list
List all options

    ./zfssnap.py --help

## Schedule snapshots
To schedule snapshots crontab are normally used. This is an example root
crontab for this purpose:

    */15 *      *  *  *   /usr/local/sbin/zfssnap --log-level WARNING --policy snapshot-all
    */5  *      *  *  *   /usr/local/sbin/zfssnap --log-level WARNING --policy replicate-vms

* `zfssnap.py` has been symlinked to `/usr/local/sbin/zfssnap` for ease of use.
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

## Development
### Run tests
Install pytest (preferably in a virtual environment):

    pip3 install pytest pytest-cov
Ensure you are standing in the project root:

    PYTHONPATH=src/ pytest --cov=zfssnap
