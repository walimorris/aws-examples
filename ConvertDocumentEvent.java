import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3ObjectEntity;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Image;
import com.itextpdf.text.pdf.PdfWriter;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class ConvertDocumentEvent {
    private final String PDF = ".pdf";
    private final String JPEG = ".jpeg";
    private final String JPG = ".jpg";
    private final String GIF = ".gif";
    private final String TIFF = ".tiff";
    private final String PNG = ".png";
    private final String TMP = "/tmp/";
    private final String BUCKET = System.getenv("BUCKET_NAME");

    public String handleRequest(S3Event event, Context context) throws IOException, DocumentException {
        LambdaLogger logger = context.getLogger();
        List<String> imageFiles = Arrays.asList(JPEG, JPG, GIF, TIFF, PNG);

        S3EventNotificationRecord eventRecord = event.getRecords().get(0);
        S3ObjectEntity file = eventRecord.getS3().getObject();
        String originalFileName = file.getKey();
        S3Object fileObject = getS3Object(getS3Client(), BUCKET, originalFileName);
        String fileExtension = getFileExtension(originalFileName);
        String fileNameWithoutExtension = getFileNameWithoutExtension(originalFileName);

        if (!fileExtension.equalsIgnoreCase(PDF)) {

            // case: document is uploaded as an image file. Often users will upload pictures
            // of documents in image format, and here we want to convert this image file to
            // a proper pdf for redacting info later
            if (imageFiles.contains(fileExtension)) {
                // send S3Object to tmp dir
                File s3ObjectAsFile = new File(TMP + originalFileName);

                storeS3ObjectAsFile(s3ObjectAsFile, fileObject);
                File convertedFile = convertImageFileToPDF(s3ObjectAsFile.getPath(), fileNameWithoutExtension);
                if (convertedFile != null) {

                    // converted file is sitting in /tmp now we need to write it to s3
                    // and delete the original non .pdf file from s3
                    putConvertedFileS3(convertedFile, originalFileName);
                }
            }
        }
        return "success";
    }

    /**
     * Find the given file's format extension by splitting the file name and pulling the
     * last characters after the final split.
     *
     * @param fileName name of given file
     * @return {@link String} file extension. ex: .pdf, .png, etc
     */
    private String getFileExtension(String fileName) {
        String[] fileExtension = fileName.split("\\.");
        return String.format(".%s", fileExtension[fileExtension.length -1]);
    }

    /**
     * Split the original file name and remove the extension.
     *
     * @param fullFileName full filename ex: /tmp/fileName.jpeg
     * @return {@link String} the file name without extension
     */
    private String getFileNameWithoutExtension(String fullFileName) {
        String[] fileSplit = fullFileName.split("\\.");
        return fileSplit[0];
    }

    /**
     * Get a single {@link S3Object}
     *
     * @param client {@link AmazonS3} client
     * @param bucketName bucket name
     * @param keyName key name
     * @return {@link S3Object}
     */
    private S3Object getS3Object(AmazonS3 client, String bucketName, String keyName) {
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, keyName);
        return client.getObject(getObjectRequest);
    }

    /**
     * Get {@link AmazonS3} client
     * @return {@link AmazonS3}
     */
    private AmazonS3 getS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();
    }

    /**
     * Stores the {@link S3Object} content into a file a temporary file stored in /tmp folder.
     *
     * @param tmpFile path to /tmp to store file
     * @param fileObject {@link S3Object} will be read and content will be written to /tmp file
     * @throws IOException Upon {@link IOException}
     */
    private void storeS3ObjectAsFile(File tmpFile, S3Object fileObject) throws IOException {
        OutputStream out = Files.newOutputStream(tmpFile.toPath());
        InputStream in = fileObject.getObjectContent();
        IOUtils.copy(in, out);
        in.close();
        out.close();
    }

    /**
     * Converts an image file to PDF.
     *
     * @param fileNameWithExtension complete original file name with extension
     * @param fileNameWithoutExtension original file name without extension
     *
     * @return {@link File}
     *
     * @throws IOException
     * @throws DocumentException
     */
    private File convertImageFileToPDF(String fileNameWithExtension, String fileNameWithoutExtension)
            throws IOException, DocumentException {

        // Write the pdf file to /tmp dir with original content now as .pdf file type
        Document document = new Document();
        String outputFile = TMP + fileNameWithoutExtension + PDF;

        FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
        PdfWriter writer = PdfWriter.getInstance(document, fileOutputStream);
        writer.open();
        document.open();

        // Get original image file byte array
        File imageFile = new File(fileNameWithExtension);
        byte[] imageFileContent = Files.readAllBytes(imageFile.toPath());

        // use image byte array to add to pdf document
        document.add(Image.getInstance(imageFileContent));
        document.close();
        writer.close();

        if (Files.exists(Paths.get(outputFile))) {
            return new File(outputFile);
        }
        return null;
    }

    /**
     * Puts converted file to s3 and deletes the original file.
     *
     * @param convertedFile {@link File} converted file from original
     * @param originalFileName {@link String} file name of original converted file
     */
    private void putConvertedFileS3(File convertedFile, String originalFileName) {
       getS3Client().putObject(BUCKET, convertedFile.getName(), convertedFile);
       deleteFileS3(originalFileName);

    }

    /**
     * Deletes Object in S3.
     *
     * @param fileName name of file to delete
     */
    private void deleteFileS3(String fileName) {
        getS3Client().deleteObject(BUCKET, fileName);
    }
}
