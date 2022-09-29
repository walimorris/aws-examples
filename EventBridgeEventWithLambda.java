import com.amazonaws.AmazonClientException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.AmazonSNSException;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

import java.util.Map;

/**
 * <p>
 * Description: This POC goes through the idea of processing some Lambda Function code as
 * part of a Scheduled EventBridge Event. In this POC the details of setting up a trigger
 * (EventBridge Event) based on state changes for a EC2 instance. The event is scheduled to
 * trigger based on the state change of a EC2 instance. Once certain states are detected
 * our EventBridge rule will trigger lambda to restart that instance and send an email with
 * SNS to anyone subscribed to the topic.
 * </p>
 * <p></p>
 * <p>
 * Step 1: Create a role that provides Lambda function access to CloudWatch, EventBridge, EC2,
 * SNS, and basic Lambda execution. Also, have a running instance which should be one that should
 * not be shut down. Here's an example CloudFormation Template to create the Lambda
 * Function (with path to code), a role with the necessary permissions (you can limit access
 * based on least privilege). This code should be placed inside a 'template.yml' file which
 * can be used with sam deploy:
 * </p>
 *
 * <pre>
 * AWSTemplateFormatVersion: 2010-09-09
 * Transform: AWS::Serverless-2016-10-31
 * Description: <description> template
 *
 * Parameters:
 *   RootRole:
 *     Description: role for event
 *     Type: String
 *     Default: <RoleName>
 *
 * Globals:
 *   Function:
 *     Runtime: java8
 *     MemorySize: 512
 *     Timeout: 25
 *
 * Resources:
 *   RoleName:
 *     Type: AWS::IAM::Role
 *     Properties:
 *       AssumeRolePolicyDocument:
 *         Version: "2012-10-17"
 *         Statement:
 *           - Effect: Allow
 *             Principal:
 *               Service:
 *                 - lambda.amazonaws.com
 *             Action: sts:AssumeRole
 *       Description: Role to provide access to s3, SNS, cloudwatch, ec2, EventBridge and basic lambda execution
 *       ManagedPolicyArns:
 *         - arn:aws:iam::aws:policy/AmazonS3FullAccess
 *         - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
 *         - arn:aws:iam::aws:policy/AWSCloudFormationFullAccess
 *         - arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess
 *         - arn:aws:iam::aws:policy/AmazonEC2FullAccess
 *         - arn:aws:iam::aws:policy/AmazonSNSFullAccess
 *       RoleName: !Ref RootRole
 *
 *   YourFunctionName:
 *     Type: AWS::Serverless::Function
 *     Properties:
 *       CodeUri: target/lambda.zip
 *       Role: !GetAtt <RoleName>.Arn
 *       Handler: org.example.events.<YourFunctionName>::handleRequest
 * </pre>
 * <p></p>
 * Step 2: Once you've deployed the Lambda Function with the Serverless Application Model or
 * have created the proper role and uploaded the Lambda Function. Go to EventBridge and create
 * a new rule. During creation you'll add the Lambda function as the target (when the rule pattern
 * is matched the lambda function will be triggered). Your event pattern would look similar to:
 * <p></p>
 * <pre>
 *   "version": ["0"],
 *   "source": ["aws.ec2"],
 *   "account": ["1234567891012"],
 *   "detail-type": ["EC2 Instance State-change Notification"],
 *   "region": ["us-west-2"],
 *   "resources": ["arn:aws:ec2:us-west-2:1234567891012:instance/i-0fc12345j001ce33"],
 *   "detail": {
 *     "instance-id": ["i-i-0fc12345j001ce33"],
 *     "state": ["shutting-down", "stopping", "stopped"]
 *   }
 * }
 * </pre>
 * <p></p>
 * <p>
 * Step 3 (Optional): Create an SNS topic and add the email address of the subscribers to the topic.
 * The Lambda Function will restart the instance so it's running and notify any subscribers of the
 * actions.
 * </p>
 */
public class MonitoredInstanceEvent {
    private final String SNS_TOPIC_ARN = "arn:aws:sns:us-west-2:123456789123:yourTopicName";
    private final String INSTANCE_ID = "instance-id";

    /**
     * Lambda Function will handle the event from EventBridge when pattern details from
     * EC2 instance is met. In this case, we don't want our monitoring system to have
     * any downtime so if certain instance state details are met, we will activate a run command
     * for the monitoring instance and notify subscribers to the SNS topic.
     *
     * @param event {@link ScheduledEvent} notice what class handles EventBridge events
     * @param context {@link Context}
     *
     * @return {@link String}
     */
    public String handleRequest(ScheduledEvent event, Context context) {

        LambdaLogger logger = context.getLogger();

        Map<String, Object> details = event.getDetail();

        // get instance-id and region (used to send ec2 start request)
        String region = event.getRegion();
        String instanceId = (String) details.get(INSTANCE_ID);

        AmazonEC2 ec2Client = AmazonEC2Client.builder()
                .withRegion(region)
                .build();

        StartInstancesRequest startInstancesRequest = new StartInstancesRequest()
                .withInstanceIds(instanceId);

        /*
         EC2 service will complain about the instance being in such a state that the instance can not be
         started. We will handle this with a try block, ignoring the exception because Lambda will re-try
         two more times as a default. This gives the instance enough time to update its state to a state
         that allows our code to be processed. Our code is written so that if it is processed multiple
         times, it won't affect the intended result of restarting the instance.
         */
        try {
            StartInstancesResult startInstancesResult = ec2Client.startInstances(startInstancesRequest);
            String startingInstanceId = startInstancesResult.getStartingInstances()
                    .get(0)
                    .getInstanceId();

            if (startingInstanceId.equals(instanceId)) {
                String restartNote = String.format("Instance: '%s' was shutdown...restarting", startingInstanceId);
                logger.log(restartNote);

                // attach SNS and send notification to admin or sys ops
                PublishResult snsPublishResult = publishMessage(restartNote, region, logger);
                if (snsPublishResult == null) {
                    logger.log(String.format("Unable to send SNS notification for topic: %s", SNS_TOPIC_ARN));
                }
            }
        } catch (AmazonClientException e) {
            logger.log("Amazon State Exception is ignored, Instance will start up as planned on retry");
        }
        ec2Client.shutdown();
        return "success";
    }

    /**
     * This function will publish an SNS Notification to all subscribers to the given Topic Arn.
     * @param message notification message
     * @param region SNS topic region
     * @param logger current {@link LambdaLogger}
     *               
     * @return {@link PublishResult}
     */
    private PublishResult publishMessage(String message, String region, LambdaLogger logger) {
        AmazonSNS snsClient = AmazonSNSClient.builder()
                .withRegion(region)
                .build();
        try {
            PublishRequest publishRequest = new PublishRequest()
                    .withTopicArn(SNS_TOPIC_ARN)
                    .withMessage(message);
            
            PublishResult publishResult = snsClient.publish(publishRequest);
            snsClient.shutdown();          
            return publishResult;
            
        } catch (AmazonSNSException e) {
            logger.log(String.format("Unable to process notification for topic: %s", SNS_TOPIC_ARN));
            if (snsClient != null) {
                snsClient.shutdown();
            }
        }
        return null;
    }
}
