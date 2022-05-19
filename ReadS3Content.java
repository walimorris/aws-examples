import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;


/**
 * This example creates an AmazonS3Client (with credentials from AWS CLI), finds a specific S3Bucket, iterates the objects in that 
 * S3Bucket and looks for a specific file. Once that file is found, the contents of that file is parsed to a S3ObjectInputStream 
 * and fed to a BufferedReader in order to build a String containing that files content. The final content is than printed to STDOUT
 * for examination.
 */
public class App {
    public static void main( String[] args ) {
        final String OG_FLAKES_REPO = "ogflakes-repo";
        final String SNS_EMAIL_SOURCE_CODE_FILE = "src/main/java/com/morris/ogflakes/model/SnsEmail.java";
        AmazonS3 amazonS3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();

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
    }
}
