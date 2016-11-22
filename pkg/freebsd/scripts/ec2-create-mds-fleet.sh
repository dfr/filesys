#! /bin/sh

image=ami-6ceaab7b
subnet=subnet-ddefaef7
sg=sg-f0ff738b
userdata="$(sed -e 's/%ROLE%/METADATA/' ./pkg/freebsd/scripts/ec2-install.sh)"

mds0=172.31.65.0
mds1=172.31.65.1
mds2=172.31.65.2

getInstanceId() {
    echo $1 > foo.json
    python <<EOF
import json
import sys
obj = json.load(open('foo.json'))
print obj['Instances'][0]['InstanceId']
EOF
}

ids=""
for i in 0 1 2; do
    output=$(aws ec2 run-instances \
	--image-id ${image} \
	--subnet-id ${subnet} \
	--private-ip-address $(eval echo '$mds'$i) \
	--count 1 \
	--iam-instance-profile "Name=unfsd-test" \
	--instance-type c3.xlarge \
	--key-name nfs@rabson.org \
	--security-group-ids ${sg} \
	--user-data "${userdata}")
    id=$(getInstanceId "${output}")
    aws ec2 create-tags \
	--resources ${id} \
	--tags Key=Type,Value=unfsd-mds
done
