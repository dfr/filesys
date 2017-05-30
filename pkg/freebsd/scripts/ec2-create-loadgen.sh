#! /bin/sh

image=ami-b63769a1
subnet=subnet-ddefaef7
sg=sg-f0ff738b
userdata="$(cat ./scripts/linux-loadgen.sh)"

n=1
if [ $# -gt 0 ]; then
    n=$1
    shift
fi

getInstanceId() {
    echo $1 > foo.json
    python <<EOF
import json
import sys
obj = json.load(open('foo.json'))
for i in obj['Instances']:
    print i['InstanceId'],
print
EOF
}

output=$(aws ec2 run-instances \
	     --image-id ${image} \
	     --subnet-id ${subnet} \
	     --count $n \
	     --iam-instance-profile "Name=unfsd-test" \
	     --instance-type t2.micro \
	     --key-name nfs@rabson.org \
	     --security-group-ids ${sg} \
	     --instance-initiated-shutdown-behavior terminate \
	     --user-data "${userdata}")
id=$(getInstanceId "${output}")
aws ec2 create-tags \
    --resources ${id} \
    --tags Key=Type,Value=loadgen
