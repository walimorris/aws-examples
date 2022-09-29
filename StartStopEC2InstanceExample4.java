import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;


/**
 * Prerequisites:
 * 1. AWS Credentials setup in console
 *
 * In this example, we build a EC2 instance with Micro T2 instance type with the Ubuntu AMI in 
 * US_WEST_2 Region. By default this will use your accounts default VPC in that region. This is
 * a basic example and I suggest you refer to ec2 java sdk if you'd like to setup more properties
 * for your instance. Once the instance is started, the console will pend until you type 'close'
 * to shutdown your EC2 instance. I suggest you have the console open to keep eyes on your instance/s.
 * As always, don't forget to shutdown the instance when you're done, if the code throws an exception
 * and does not do it for you. Refer to methods launchEC2InstanceInDefaultVPC() and stopEC2Instance()
 * 
 * Cheers!
 */
public class App {
    public static void main( String[] args ) {
        AmazonS3 amazonS3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();

        AmazonEC2 amazonEC2Client = AmazonEC2Client.builder()
                .withRegion(Regions.US_WEST_2)
                .build();

        // replication rule name for the original bucket
        final String originalReplicationRuleName = "***Replication Rule***";
        Bucket originalBucket = getBucket(amazonS3Client, "***BucketName***");
        Bucket copyBucket = listOriginalBucketObjectsAndPrintSource(amazonS3Client);
        Bucket copyBackupBucket = createCopyBackup(amazonS3Client);

        if (copyBackupBucket != null) {
            System.out.println("bucket ready: " + copyBackupBucket.getName());

            // while we're here let's set the bucket policy
            if (amazonS3Client.getBucketPolicy(copyBackupBucket.getName()) == null) {
                BucketPolicy originalBucketPolicy = amazonS3Client.getBucketPolicy(copyBucket.getName());
                if (originalBucketPolicy != null) {
                    amazonS3Client.setBucketPolicy(copyBackupBucket.getName(), originalBucketPolicy.getPolicyText());
                }
            }
        }

        // Lets enable versioning on our new bucket as versioning must be enabled on destination bucket as well
        if (copyBackupBucket != null) {
            BucketVersioningConfiguration enabledVersioningConfig = new BucketVersioningConfiguration().withStatus(BucketVersioningConfiguration.ENABLED);
            SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest = new SetBucketVersioningConfigurationRequest(
                    copyBackupBucket.getName(), enabledVersioningConfig);
            amazonS3Client.setBucketVersioningConfiguration(setBucketVersioningConfigurationRequest);
            BucketVersioningConfiguration copyBackupVersioningConfig = amazonS3Client.getBucketVersioningConfiguration(copyBackupBucket.getName());
            if (copyBackupVersioningConfig.getStatus().equals(BucketVersioningConfiguration.ENABLED)) {
                try {
                    // let's replicate our original repo copy objects to the backup
                    // ensure the backup bucket is empty, we don't want to double dip
                    if (amazonS3Client.listObjects(copyBackupBucket.getName()).getObjectSummaries().size() == 0) {
                        String accountId = "***Bucket Owner Account ID***";
                        String roleName = "***Role Name for replication rule***";
                        // original bucket is source bucket
                        String sourceBucketName = originalBucket.getName();
                        // backup bucket is destination bucket
                        String destBucketName = copyBackupBucket.getName();

                        String roleARN = String.format("arn:aws:iam::%s:role/%s", accountId, roleName);
                        String destinationBucketARN = "arn:aws:s3:::" + destBucketName;

                        if (originalBucket != null) {
                            // Let's get the original bucket's replication config, get the rules to obtain the destination config rule, and update the bucket arn to our
                            // new destination bucket(backup) then assign the destination config rule back to the origin bucket's replication config. This way we're not building
                            // an entirely new replication config
                            BucketReplicationConfiguration originalBucketReplicationConfiguration = amazonS3Client.getBucketReplicationConfiguration(originalBucket.getName());
                            ReplicationDestinationConfig destinationConfig = originalBucketReplicationConfiguration.getRules()
                                .get(originalReplicationRuleName)
                                .getDestinationConfig();
                            
                            destinationConfig.setBucketARN(destinationBucketARN);
                            originalBucketReplicationConfiguration.getRules()
                                .get(originalReplicationRuleName)
                                .setDestinationConfig(destinationConfig);

                            // Save the replication rule to the source bucket.
                            amazonS3Client.setBucketReplicationConfiguration(sourceBucketName,
                                    new BucketReplicationConfiguration()
                                            .withRoleARN(roleARN)
                                            .withRules(originalBucketReplicationConfiguration.getRules()));
                        }
                        System.out.println("Replication config processed with new destination source");
                    } else {
                        System.out.println("Objects have already been replicated to destination bucket: " + copyBackupBucket.getName());
                    }
                } catch (AmazonServiceException e) {
                    System.out.println("There was an error processing request to build Replication Config with new destination: " + e.getMessage());
                }
            }
        }

        final String UBUNTU_AMI = "ami-0ee8244746ec5d6d4";
        final String KEY_NAME = "***Key Pair Name***";
        String defaultEC2Instance = launchEC2InstanceInDefaultVPC(amazonEC2Client, UBUNTU_AMI, KEY_NAME);
        if (defaultEC2Instance != null) {
            System.out.println("EC2 instance with id '" + defaultEC2Instance + "' started");
            Scanner scanner = new Scanner(System.in);
            System.out.println("Type 'close' to shut down instance with id: " + defaultEC2Instance);
            String closeStr = scanner.nextLine();
            if (closeStr.equals("close")) {
                boolean instanceStopping = stopEC2Instance(amazonEC2Client,defaultEC2Instance);
                if (instanceStopping) {
                    System.out.println("Instance stopping with id: " + defaultEC2Instance);
                } else {
                    System.out.println("instance not stopping, check AWS console manually for instance: " + defaultEC2Instance);
                }
            }
        }

    }

    private static Bucket listOriginalBucketObjectsAndPrintSource(AmazonS3 amazonS3Client) {
        final String ORIGINAL_REPO = "***use copied bucket from original bucket***";
        final String SNS_EMAIL_SOURCE_CODE_FILE = "***Path to source file***";

        Bucket originalBucket = getBucket(amazonS3Client, ORIGINAL_REPO);
        S3ObjectInputStream snsSourceCodeInputStream = null;
        if (originalBucket != null) {
            try {
                ObjectListing originalBucketObjectListings = amazonS3Client.listObjects(originalBucket.getName());
                if (originalBucketObjectListings.getObjectSummaries().size() > 0) {
                    for (S3ObjectSummary summary : originalBucketObjectListings.getObjectSummaries()) {
                        if (summary.getKey().equals(SNS_EMAIL_SOURCE_CODE_FILE)) {
                            S3Object sourceCodeObject = amazonS3Client.getObject(ORIGINAL_REPO, summary.getKey());
                            snsSourceCodeInputStream = sourceCodeObject.getObjectContent();
                            break;
                        }
                    }
                }
            } catch (AmazonS3Exception e) {
                System.out.println("Error getting s3 bucket and printing summaries: " + e.getMessage());
            }
        }
        StringBuilder sourceCode = new StringBuilder();
        if (snsSourceCodeInputStream != null) {
            BufferedReader snsSourceCodeBufferedReader = new BufferedReader(new InputStreamReader(snsSourceCodeInputStream));
            try {
                String line = snsSourceCodeBufferedReader.readLine();
                while (line != null) {
                    sourceCode.append(line);
                    sourceCode.append("\n");
                    line = snsSourceCodeBufferedReader.readLine();
                }
            } catch (IOException e) {
                System.out.println("Error reading object: " + SNS_EMAIL_SOURCE_CODE_FILE);
            }
        }
        System.out.println(sourceCode);
        return originalBucket;
    }

    private static Bucket createCopyBackup(AmazonS3 amazonS3Client) {
        final String REPO_COPY = "***Use original buckets copied bucket***";
        final String BACKUP_SUFFIX = "-backup";

        Bucket repoCopyBackupBucket = null;

        String repoCopyBackupBucketName = StringUtils.join(REPO_COPY, BACKUP_SUFFIX);
        if (!amazonS3Client.doesBucketExistV2(repoCopyBackupBucketName)) {
            // create repo copy backup bucket
            try {
                repoCopyBackupBucket = amazonS3Client.createBucket(repoCopyBackupBucketName);
            } catch (AmazonS3Exception e) {
                System.out.println("Error creating bucket '" + repoCopyBackupBucketName + "': " + e.getMessage());
            }
        } else {
            // bucket exists - get bucket
            System.out.println(repoCopyBackupBucketName + "exists! Assigning to bucket variable.");
            repoCopyBackupBucket = getBucket(amazonS3Client, repoCopyBackupBucketName);
        }
        return repoCopyBackupBucket;
    }

    private static Bucket getBucket(AmazonS3 awsClient, String bucketName) {
        Bucket bucket = null;
        for (Bucket b : awsClient.listBuckets()) {
            if (b.getName().equals(bucketName)) {
                bucket = b;
                break;
            }
        }
        return bucket;
    }

    private static String launchEC2InstanceInDefaultVPC(AmazonEC2 amazonEC2, String ami, String keyName) {
        try {
            RunInstancesRequest runRequest = new RunInstancesRequest(ami, 1, 1)
                    .withInstanceType(InstanceType.T2Micro)
                    .withKeyName(keyName);

            RunInstancesResult runInstancesResult = amazonEC2.runInstances(runRequest);
            String instanceId = runInstancesResult.getReservation()
                .getInstances()
                .get(0)
                .getInstanceId();

            StartInstancesRequest startInstancesRequest = new StartInstancesRequest().withInstanceIds(instanceId);
            StartInstancesResult startInstancesResult = amazonEC2.startInstances(startInstancesRequest);
            return startInstancesResult.getStartingInstances()
                .get(0)
                .getInstanceId();
            
        } catch (Exception e) {
            System.out.println("Error starting Ec2 instance");
            return null;
        }
    }

    private static boolean stopEC2Instance(AmazonEC2 amazonEC2,String ec2InstanceId) {
        // stop instance by build a StopInstancesRequest and passing along the instance ID
        // returns true if instance is in stopping state, otherwise returns false
        StopInstancesRequest stopInstancesRequest = new StopInstancesRequest().withInstanceIds(ec2InstanceId);
        StopInstancesResult stopInstancesResult = amazonEC2.stopInstances(stopInstancesRequest);
        for (InstanceStateChange instance : stopInstancesResult.getStoppingInstances()) {
            if (instance.getInstanceId().equals(ec2InstanceId)) {
                return true;
            }
        }
        return false;
    }
}
