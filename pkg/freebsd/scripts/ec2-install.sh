#! /bin/sh

# Bootstrap if necessary
if ! pkg -N 2>/dev/null; then
        env ASSUME_ALWAYS_YES=YES pkg bootstrap | cat
fi

# Install awscli
env ASSUME_ALWAYS_YES=YES pkg install awscli sudo bash </dev/null | cat

# Enable sudo for wheel group
echo '%wheel        ALL=(ALL) NOPASSWD: SETENV: ALL' > /usr/local/etc/sudoers.d/wheel

# Install the binaries and scripts
/usr/local/bin/aws s3 cp s3://rabson-org-dist/dist.tar.gz /tmp
cd /
tar xvf /tmp/dist.tar.gz

# We override the storage fsid so that we can present a consistent
# identifier if the contents of our ephemeral disks are lost
fsid=`uuidgen | tr -d '-'`

# Configure services. We disable ephemeralswap so that we can use any
# ephemeral disks for storage. The actual storage is configured in the
# unfsd startup script since it may disappear if the instance
# restarts.
cat >> /etc/rc.conf <<EOF
ec2_ephemeralswap_enable="NO"
unfsd_enable="YES"
unfsd_storage="/storage"
unfsd_ec2_storage="YES"
unfsd_flags="--daemon --listen=$(hostname) --threads=8 --realm=vpc.rabson.org --fsid=${fsid}"
unfsd_mds_addr="udp://mds.vpc.rabson.org:2049"
unfsd_role="%ROLE%"
unfsd_paxos="YES"
unfsd_paxos_replicas="udp://mds.vpc.rabson.org:20490"
EOF

# Configure storage
mkdir /storage

# We need to reboot since we added a startup script
touch /firstboot-reboot
