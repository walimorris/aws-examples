import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

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
 * up more than 1 kilobyte of storage and impacted resources in US_WEST_2 Region. Later, we take a single 
 * file and parse its content to STDOUT. What's interesting about this is that CloudTrail logs will be 
 * stored in S3 buckets as GZIP files with its content in json format. Pretty cool since we can then do 
 * something with this information. This shows how awesome, thoughtful and powerful AWS is; especially with 
 * exchanging information. We could do many things with this information such as building notifications, 
 * alerts, analytics and much more. So, how are we supposed to actually read a gzip file!? Java solves this 
 * small problem with GZIPInputStreams!
 */
public class Main {
    public static final String CLOUD_TRAIL_LOGS = "aws-cloudtrail-logs";

    public static void main(String[] args) {
        Bucket cloudTrailBucket = null;
        JSONObject cloudTrailLogJSON = null;
        AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();
        List<Bucket> allBuckets = amazonS3.listBuckets();
        for (Bucket bucket : allBuckets) {
            if (bucket.getName().contains(CLOUD_TRAIL_LOGS)) {
                cloudTrailBucket = bucket;
            }
        }

        if (cloudTrailBucket != null) {
            List<S3ObjectSummary> logSummaries = getUsWestLogSummaries(amazonS3, cloudTrailBucket);
            if (logSummaries.size() > 0) {
                cloudTrailLogJSON = getLogJsonFromGZIP(amazonS3, cloudTrailBucket, logSummaries);
            }
        }

        // pretty print the log in json format
        if (cloudTrailLogJSON != null && !cloudTrailLogJSON.isEmpty()) {
            System.out.println(cloudTrailLogJSON.toString(2));
        }
    }

    private static List<S3ObjectSummary> getUsWestLogSummaries(AmazonS3 amazonS3, Bucket cloudTrailBucket) {
        if (cloudTrailBucket != null) {
            System.out.printf("Found Cloud Trail Log Bucket: %s", cloudTrailBucket.getName());
        } else {
            System.out.println("Couldn't find a bucket with Cloud Trail Logs");
            System.exit(1);
        }
        List<S3ObjectSummary> logSummaries = new ArrayList<>();
        ObjectListing bucketObjectListings = amazonS3.listObjects(cloudTrailBucket.getName());
        for (S3ObjectSummary summary : bucketObjectListings.getObjectSummaries()) {
            // print objects in US West 2 region over 1 kilobyte in size
            if (summary.getKey().contains(Regions.US_WEST_2.getName()) && summary.getSize() >= 1024) {
                logSummaries.add(summary);
                System.out.printf(summary.getKey() + " = %s\n", summary.getSize());
            }
        }
        return logSummaries;
    }

    private static JSONObject getLogJsonFromGZIP(AmazonS3 amazonS3, Bucket cloudTrailBucket, List<S3ObjectSummary> logSummaries) {
        S3Object log = amazonS3.getObject(cloudTrailBucket.getName(), logSummaries.get(0).getKey());
        ObjectMetadata logMetaData = log.getObjectMetadata();

        System.out.printf("logencoding=%s,logContentType=%s,KMSKeyId=%s\n", logMetaData.getContentEncoding(),
                logMetaData.getContentType(), logMetaData.getSSEAwsKmsKeyId());

        // cloud trail log files will be gzip encoded with json content type. Luckily Java has a solution for this
        S3ObjectInputStream logStream = log.getObjectContent();
        JSONObject logJson = null;
        try {
            BufferedReader logReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(logStream)));
            StringBuilder logBuilder = new StringBuilder();
            String line = logReader.readLine();
            while (line != null) {
                logBuilder.append(line);
                line = logReader.readLine();
            }
            logJson = new JSONObject(logBuilder.toString());
        } catch (IOException e) {
            System.out.println("Error reading content from GZIP file");
        } catch (JSONException e) {
            System.out.printf("Error building JSONObject from log gzip file: %s\n", log.getKey());
        }
        return logJson;
    }
}
