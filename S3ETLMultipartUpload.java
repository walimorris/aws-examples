package org.s3etl;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        // enable S3 Acceleration - typically used for globally distributed (or ingestion)
        // networks for faster content upload and retrieval
        S3Configuration s3Configuration = S3Configuration.builder()
                .accelerateModeEnabled(true)
                .build();

        S3Client s3Client = S3Client.builder()
                .region(Region.US_WEST_2)
                .serviceConfiguration(s3Configuration)
                .build();

        CreateMultipartUploadRequest multipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket("BUCKETNAME")
                .key("multipart-uploads/example1.txt")
                .build();

        CreateMultipartUploadResponse multipartUploadResponse = s3Client.createMultipartUpload(multipartUploadRequest);
        String uploadId = multipartUploadResponse.uploadId();

        // Multipart Uploads have a minimum of 5mb size, anything smaller can not be used for multipart upload
        String[] partPaths = {"/home/user/mp/part1.txt", "/home/user/mp/part2.txt", "/home/user/mp/part3.txt" };

        CompleteMultipartUploadResponse completeMultipartUploadResponse = buildMultiPartTextUpload(s3Client,
                "BUCKETNAME", "multipart-uploads/example1.txt", uploadId, partPaths.length, partPaths);

        // looking for 200 status code - check bucket to ensure a single txt file has been created containing each part
        System.out.println(completeMultipartUploadResponse.sdkHttpResponse().statusCode());
    }

    public static CompleteMultipartUploadResponse buildMultiPartTextUpload(S3Client s3Client, String bucket, String key,
                                                                           String uploadId, int parts, String[] paths) {
       
        // maintains an ordered list containing each part for the upload 
        List<CompletedPart> completedParts = new ArrayList<>();
        for (int i = 0; i < parts; i++) {

            // build each part with the given path
            UploadPartRequest partRequest = UploadPartRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadId)
                    .partNumber(i+1)
                    .build();

            String etag = s3Client.uploadPart(partRequest, Paths.get(paths[i])).eTag();
            CompletedPart completedPart = CompletedPart.builder()
                    .partNumber(i+1)
                    .eTag(etag)
                    .build();

            completedParts.add(completedPart);
        }
        
        // use each completed part 
        CompletedMultipartUpload multipartUpload = CompletedMultipartUpload.builder()
                .parts(completedParts)
                .build();

        // use bucket info and the built multipart upload
        CompleteMultipartUploadRequest multipartUploadRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(multipartUpload)
                .build();

        return s3Client.completeMultipartUpload(multipartUploadRequest);
    }
}
