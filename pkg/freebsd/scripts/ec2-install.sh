#! /bin/sh

# Bootstrap if necessary
if ! pkg -N 2>/dev/null; then
        env ASSUME_ALWAYS_YES=YES pkg bootstrap | cat
fi

# Install awscli
env ASSUME_ALWAYS_YES=YES pkg install awscli </dev/null | cat

# Install the binaries and scripts
/usr/local/bin/aws s3 cp s3://rabson-org-dist/dist.tar.gz /tmp
cd /
tar xvf /tmp/dist.tar.gz

# Configure services
cat >> /etc/rc.conf <<EOF
unfsd_enable="YES"
unfsd_storage="/storage"
unfsd_flags="--daemon --grace_time=0 --threads=8"
unfsd_mds_addr="udp://mds.vpc.rabson.org:2049"
unfsd_role="DATA"
EOF

# Configure storage
mkdir /storage

# We need to reboot since we added a startup script
touch /firstboot-reboot
