1. Bootstrap script: 
#!/bin/bash
yum update -y

2. Install the CloudWatch Agent: 
sudo yum install amazon-cloudwatch-agent -y

3. Configure the CloudWatch agent: 
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-config-wizard

**** Say no to monitoring CollectD ****
**** Monitor /var/log/messages ****

4. cd /opt/aws/amazon-cloudwatch-agent/bin
   /opt/aws/amazon-cloudwatch-agent/bin/config.json is the config file

5. Start the CloudWatch Agent
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/bin/config.json

6. Generate some activity on our system by installing stress - it’s in the Extra Packages for Enterprise Linux (EPEL) repository, so first we'll install the epel repository, then we'll install stress:

sudo amazon-linux-extras install epel -y
sudo yum install stress -y
stress --cpu 1
