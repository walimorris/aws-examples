#!/bin/bash

# install ssm agent Amazon Linux 2 - x86_64
sudo yum install -y https://s3.amazonaws.com/ec2-downloads-windows/SSMAgent/latest/linux_amd64/amazon-ssm-agent.rpm

# Run agent
sudo systemctl start amazon-ssm-agent

# ensure agent is running
sudo systemctl status amazon-ssm-agent
