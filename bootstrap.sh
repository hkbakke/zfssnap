#!/usr/bin/env bash

wget --no-verbose http://archive.zfsonlinux.org/debian/pool/main/z/zfsonlinux/zfsonlinux_6_all.deb
dpkg -i zfsonlinux_6_all.deb
rm -f zfsonlinux_6_all.deb
  
apt-get update
apt-get install vim python3 debian-zfs -y
modprobe zfs

zpool create -f -m /zpools/dev-1 dev-1 mirror /dev/sdb /dev/sdc
zfs set compression=lz4 dev-1
zfs set xattr=sa dev-1
zfs set acltype=posixacl dev-1
zfs create dev-1/test-1
zfs create dev-1/test-2
zfs create dev-1/test-3