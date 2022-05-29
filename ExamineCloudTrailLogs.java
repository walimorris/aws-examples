import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.List;

/**
 * Prerequisites: 
 * 1. Create a cloud trail log in your account. By default, an S3 bucket is created to store your cloud trail 
 * logs throughout your account. Logging doesn't incur cost, but you will incur cost for storing logs in 
 * S3.
 * 
 * 2. Ensure you have configured your account details on your local machine (CLI) and your profile is usable. 
 * 
 * In this example, we create an S3Client in US_WEST_2 region to find our aws-cloudtrail-logs bucket that 
 * contains the logs for any api calls, resources requested, updated, etc. to report bucket logs that take
 * up more than 1 kilobyte of storage and impacted resources in US_WEST_2 Region.
 */
public class Main {
    public static void main(String[] args) {
        AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();

        Bucket cloudTrailBucket = null;
        List<Bucket> allBuckets = amazonS3.listBuckets();
        for (Bucket bucket : allBuckets) {
            if (bucket.getName().contains("aws-cloudtrail-logs")) {
                cloudTrailBucket = bucket;
            }
        }
        if (cloudTrailBucket != null) {
            System.out.printf("Found Cloud Trail Log Bucket: %s", cloudTrailBucket.getName());
        } else {
            System.out.println("Couldn't find a bucket with Cloud Trail Logs");
            System.exit(1);
        }
        ObjectListing bucketObjectListings = amazonS3.listObjects(cloudTrailBucket.getName());
        for (S3ObjectSummary summary : bucketObjectListings.getObjectSummaries()) {
            // print objects in US West 2 region over 1 kilobyte in size
            if (summary.getKey().contains(Regions.US_WEST_2.getName()) && summary.getSize() >= 1024) {
                System.out.printf(summary.getKey() + " = %s\n", summary.getSize());
            }
        }
    }
}
