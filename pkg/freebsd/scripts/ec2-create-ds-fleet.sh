#! /bin/sh

subnet=subnet-ddefaef7

aws autoscaling create-auto-scaling-group \
    --auto-scaling-group-name unfsd-ds-fleet \
    --launch-configuration-name unfsd-ds-fleet \
    --min-size 5 \
    --max-size 5 \
    --vpc-zone-identifier ${subnet} \
    --tags Key=Type,Value=unfsd-ds
