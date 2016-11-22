#! /bin/sh

image=ami-6ceaab7b
subnet=subnet-ddefaef7
sg=sg-f0ff738b
userdata="$(sed -e 's/%ROLE%/DATA/' ./pkg/freebsd/scripts/ec2-install.sh)"

aws autoscaling create-launch-configuration \
    --launch-configuration-name unfsd-ds-fleet \
    --image-id ${image} \
    --security-groups ${sg} \
    --user-data "${userdata}" \
    --instance-type c3.xlarge \
    --iam-instance-profile \
    arn:aws:iam::141615839953:instance-profile/unfsd-test \
    --key-name nfs@rabson.org \
    --spot-price 0.25
