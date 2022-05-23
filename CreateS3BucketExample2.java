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
 * This example builds on 'ReadingS3Content.java'. This example creates a copy of the original existing 
 * ogflakes-repo-copy bucket (made for example purposes) and creates a new backup bucket for this original.
 * The backup bucket is assigned the same policies as the original and later we will write a replication
 * function to automate transferring the objects in the original bucket to the backup.
 */
public class App {
    public static void main( String[] args ) {
        AmazonS3 amazonS3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();

        Bucket ogFlakesRepoCopyBucket = listOgFlakesBucketObjectsAndPrintSource(amazonS3Client);
        Bucket ogFlakesRepoCopyBackupBucket = createOGFlakesRepoCopyBackup(amazonS3Client);

        if (ogFlakesRepoCopyBackupBucket != null) {
            System.out.println("bucket ready: " + ogFlakesRepoCopyBackupBucket.getName());
        }

        // get bucket policy from ogflakesRepoCopy and assign it to backup
        BucketPolicy ogFlakesRepoCopyBucketPolicy = amazonS3Client.getBucketPolicy(ogFlakesRepoCopyBucket.getName());
        if (ogFlakesRepoCopyBucketPolicy != null) {
            if (ogFlakesRepoCopyBackupBucket != null) {
                amazonS3Client.setBucketPolicy(ogFlakesRepoCopyBackupBucket.getName(), ogFlakesRepoCopyBucketPolicy.getPolicyText());
            }
        }
    }

    public static Bucket listOgFlakesBucketObjectsAndPrintSource(AmazonS3 amazonS3Client) {
        final String OG_FLAKES_REPO = "ogflakes-repo-copy";
        final String SNS_EMAIL_SOURCE_CODE_FILE = "src/main/java/com/morris/ogflakes/model/SnsEmail.java";

        List<Bucket> buckets = amazonS3Client.listBuckets();
        Bucket ogFlakesBucket = null;
        for (Bucket bucket : buckets) {
            if (bucket.getName().equals(OG_FLAKES_REPO)) {
                System.out.println(bucket.getName() + " found!");
                ogFlakesBucket = bucket;
            }
        }
        S3ObjectInputStream snsSourceCodeInputStream = null;
        if (ogFlakesBucket != null) {
            try {
                ObjectListing ogFlakesBucketObjectListings = amazonS3Client.listObjects(ogFlakesBucket.getName());
                if (ogFlakesBucketObjectListings.getObjectSummaries().size() > 0) {
                    for (S3ObjectSummary summary : ogFlakesBucketObjectListings.getObjectSummaries()) {
                        if (summary.getKey().equals(SNS_EMAIL_SOURCE_CODE_FILE)) {
                            S3Object sourceCodeObject = amazonS3Client.getObject(OG_FLAKES_REPO, summary.getKey());
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

    public static Bucket createOGFlakesRepoCopyBackup(AmazonS3 amazonS3Client) {
        final String OG_FLAKES_REPO_COPY = "ogflakes-repo-copy";
        final String BACKUP_SUFFIX = "-backup";

        Bucket ogFlakesRepoCopyBackupBucket = null;

        String ogFlakesRepoCopyBackupBucketName = StringUtils.join(OG_FLAKES_REPO_COPY, BACKUP_SUFFIX);
        if (!amazonS3Client.doesBucketExistV2(ogFlakesRepoCopyBackupBucketName)) {
            // create ogflakes repo copy backup bucket
            try {
                ogFlakesRepoCopyBackupBucket = amazonS3Client.createBucket(ogFlakesRepoCopyBackupBucketName);
            } catch (AmazonS3Exception e) {
                System.out.println("Error creating bucket '" + ogFlakesRepoCopyBackupBucketName + "': " + e.getMessage());
            }
        } else {
            // bucket exists - get bucket
            System.out.println(ogFlakesRepoCopyBackupBucketName + "exists! Assigning to bucket variable.");
            for (Bucket bucket : amazonS3Client.listBuckets()) {
                if (bucket.getName().equals(ogFlakesRepoCopyBackupBucketName)) {
                    ogFlakesRepoCopyBackupBucket = bucket;
                    break;
                }
            }
        }
        return ogFlakesRepoCopyBackupBucket;
    }
}
