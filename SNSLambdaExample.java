import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.AmazonSNSException;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

import java.util.List;

/**
 * Prerequisites:
 * 1. Setup a notification topic for cloud trail logs and add subscription parameter(email, etc).
 * 2. Through Lambda console, setup new function and ensure role has access policy to publish to
 *    notification topic.
 * 3. Add S3 trigger to Lambda function.
 * 4. Collect the Topic Arn and name for S3 bucket with cloud trail logs.
 * 5. Ensure your project is built with Maven and add the Maven Shade Plugin.
 * 6. Build using "mvn package" - the jar in target file will be used to upload in Lambda console.
 * 7. For breakdown, ensure to delete lambda function and sns topic - to limit number of notifications
 *    sent to subscriber, we've made the lambda function specific to new cloud trail logs.
 *
 * In this example we build a Lambda function that triggers on S3 uploads, validates that the upload comes
 * a specific S3 bucket which holds our cloud-trail logs, builds a custom message about the log and sends
 * the notification to all subscribers within the SNS Topic.
 *
 * *Note this is just for training purposes but it should be noted that notifications would be better
 * served for priority processes that do not occur often. Otherwise you could run up your AWS bill.
 * If you use this example please ensure to break-down all services used.
 */
public class LogSNSHandler implements RequestHandler<S3Event, String> {

    private static final String CLOUD_TRAIL_LOGS = "***S3 bucket name to cloud trail logs***";
    private static final String SNS_TOPIC_ARN = "***SNS Topic ARN***";

    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        List<S3EventNotificationRecord> s3EventNotificationRecords = s3Event.getRecords();
        if (s3EventNotificationRecords.stream().anyMatch(record -> record.getS3().getBucket().getName().contains(CLOUD_TRAIL_LOGS))) {
            StringBuilder recordsBuilder = new StringBuilder();
            recordsBuilder.append("Notification Records Count: ").append(s3EventNotificationRecords.size()).append("\n");
            for (S3EventNotificationRecord record : s3EventNotificationRecords) {
                if (record.getS3().getBucket().getName().contains(CLOUD_TRAIL_LOGS)) {
                    recordsBuilder.append("Event Name: ").append(record.getEventName()).append("\n");
                    recordsBuilder.append("Event time: ").append(record.getEventTime()).append("\n");
                    recordsBuilder.append("S3 Object Key: ").append(record.getS3().getObject().getKey()).append("\n");
                }
            }
            String message = recordsBuilder.toString();
            PublishResult result = publishMessage(message);
            if (result != null && result.getMessageId() != null) {
                return "SUCCESS";
            }
        }
        return "OK";
    }

    private PublishResult publishMessage(String message) {
        try {
            AmazonSNSClient snsClient = (AmazonSNSClient) AmazonSNSClient.builder().withRegion(Regions.US_WEST_2).build();
            PublishRequest publishRequest = new PublishRequest().withTopicArn(SNS_TOPIC_ARN).withMessage(message);
            return snsClient.publish(publishRequest);
        } catch (AmazonSNSException e) {
            System.out.println(e.getErrorMessage());
        }
        return null;
    }
}
