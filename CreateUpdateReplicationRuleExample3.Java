import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * Prerequisites:
 * 1. An original bucket with objects.
 * 2. Path of a file you'll like to print.
 * 3. A copy of the original bucket.
 * 4. Give the original bucket a policy and setup a replication configuration.
 *
 * This example builds on 'CreateS3BucketExample2.java'. This example creates a backup of the existing copy bucket 
 * (made for example purposes and based/copy of an original bucket), which contains all the objects from the original
 * bucket, and creates a new backup bucket from the copy bucket. Once the backup bucket is created, the bucket policy
 * from the original bucket is assigned to the backup. Later, we enable versioning for the copy bucket in order to 
 * change the original bucket's replication configurations destination bucket to the new backup in order to replicate
 * all objects from the original to the backup.
 */
public class App {
    public static void main( String[] args ) {
        AmazonS3 amazonS3Client = AmazonS3ClientBuilder.standard()
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
                            ReplicationDestinationConfig destinationConfig = originalBucketReplicationConfiguration.getRules().get(originalReplicationRuleName).getDestinationConfig();
                            destinationConfig.setBucketARN(destinationBucketARN);
                            originalBucketReplicationConfiguration.getRules().get(originalReplicationRuleName).setDestinationConfig(destinationConfig);

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
    }

    public static Bucket listOriginalBucketObjectsAndPrintSource(AmazonS3 amazonS3Client) {
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

    public static Bucket createCopyBackup(AmazonS3 amazonS3Client) {
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

    public static Bucket getBucket(AmazonS3 awsClient, String bucketName) {
        Bucket bucket = null;
        for (Bucket b : awsClient.listBuckets()) {
            if (b.getName().equals(bucketName)) {
                bucket = b;
                break;
            }
        }
        return bucket;
    }
}
