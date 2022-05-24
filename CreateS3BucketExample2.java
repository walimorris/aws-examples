import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;


/**
 * This example builds on 'ReadingS3ContentExample1.java'. This example creates a copy of the original existing 
 * copy bucket (made for example purposes) and creates a new backup bucket for this original.
 * The backup bucket is assigned the same policies as the original and later we will write a replication
 * function to automate transferring the objects in the original bucket to the backup.
 */
public class App {
    public static void main( String[] args ) {
        AmazonS3 amazonS3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();

        Bucket copyBucket = listBucketObjectsAndPrintSource(amazonS3Client);
        Bucket copyBackupBucket = createCopyBackup(amazonS3Client);

        if (copyBackupBucket != null) {
            System.out.println("bucket ready: " + copyBackupBucket.getName());
        }

        // get bucket policy from Copy and assign it to backup
        BucketPolicy copyBucketPolicy = amazonS3Client.getBucketPolicy(cpyBucket.getName());
        if (copyBucketPolicy != null) {
            if (copyBackupBucket != null) {
                amazonS3Client.setBucketPolicy(copyBackupBucket.getName(), copyBucketPolicy.getPolicyText());
            }
        }
    }

    public static Bucket listBucketObjectsAndPrintSource(AmazonS3 amazonS3Client) {
        final String MAINBUCKET = "***NAME OF BUCKET***";
        final String SNS_EMAIL_SOURCE_CODE_FILE = "***PATH TO SOURCE CODE***";

        List<Bucket> buckets = amazonS3Client.listBuckets();
        Bucket mainBucket = null;
        for (Bucket bucket : buckets) {
            if (bucket.getName().equals(MAINBUCKET)) {
                System.out.println(bucket.getName() + " found!");
                mainBucket = bucket;
            }
        }
        S3ObjectInputStream snsSourceCodeInputStream = null;
        if (mainBucket != null) {
            try {
                ObjectListing bucketObjectListings = amazonS3Client.listObjects(mainBucket.getName());
                if (bucketObjectListings.getObjectSummaries().size() > 0) {
                    for (S3ObjectSummary summary : bucketObjectListings.getObjectSummaries()) {
                        if (summary.getKey().equals(SNS_EMAIL_SOURCE_CODE_FILE)) {
                            S3Object sourceCodeObject = amazonS3Client.getObject(MAINBUCKET, summary.getKey());
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
        return ogFlakesBucket;
    }

    public static Bucket createCopyBackup(AmazonS3 amazonS3Client) {
        final String COPY_BUCKET = "***NAME OF COPIED BUCKET***";
        final String BACKUP_SUFFIX = "-backup";

        Bucket copyBackupBucket = null;

        String copyBackupBucketName = StringUtils.join(COPY_BUCKET, BACKUP_SUFFIX);
        if (!amazonS3Client.doesBucketExistV2(copyBackupBucketName)) {
            // create copy backup bucket
            try {
                ogFlakesRepoCopyBackupBucket = amazonS3Client.createBucket(copyBackupBucketName);
            } catch (AmazonS3Exception e) {
                System.out.println("Error creating bucket '" + copyBackupBucketName + "': " + e.getMessage());
            }
        } else {
            // bucket exists - get bucket
            System.out.println(copyBackupBucketName + "exists! Assigning to bucket variable.");
            for (Bucket bucket : amazonS3Client.listBuckets()) {
                if (bucket.getName().equals(copyBackupBucketName)) {
                    copyBackupBucket = bucket;
                    break;
                }
            }
        }
        return copyBackupBucket;
    }
}
