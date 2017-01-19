#!/usr/bin/env bash

export DEBIAN_FRONTEND=noninteractive
echo "deb http://httpredir.debian.org/debian jessie-backports main contrib" \
    > /etc/apt/sources.list.d/backports.list

apt-get update
apt-get install vim python3 python3-pip python3-yaml linux-headers-amd64 -y
apt-get install -t jessie-backports zfs-dkms zfs-zed -y

zpool create -f -m /zpools/dev-1 dev-1 mirror /dev/sdb /dev/sdc
zfs set compression=lz4 dev-1
zfs set xattr=sa dev-1
zfs set acltype=posixacl dev-1
zfs create dev-1/test-1
zfs create dev-1/test-2
zfs create dev-1/test-3
zpool create -f -m /zpools/dev-2 dev-2 mirror /dev/sdd /dev/sde
zfs set compression=lz4 dev-2
zfs set xattr=sa dev-2
zfs set acltype=posixacl dev-2
zfs create dev-2/backup

pip3 install pytest pytest-cov scandir pylint
