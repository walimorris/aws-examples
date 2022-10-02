import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.elasticfilesystem.AmazonElasticFileSystem;
import com.amazonaws.services.elasticfilesystem.AmazonElasticFileSystemClient;
import com.amazonaws.services.elasticfilesystem.model.*;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClient;
import com.amazonaws.services.simplesystemsmanagement.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class App {
    public static void main( String[] args ) throws InterruptedException {
        System.out.println(args.length);

        String fileSystemName = args[0]; // args[0]
//        List<String> addresses = new ArrayList<>(Arrays.asList(args).subList(1, args.length));
        Map<String, String> efs = getCustomEFSInfo(fileSystemName);
        List<MountTargetDescription> targetDescriptions = mountTargetDescriptions(efs);
        Map<String, String> mountTargets = getMountTargetsDetails(targetDescriptions);

        List<String> instanceIds = makeListOfInstanceIds(args);
        Map<String, String> instanceMap = getInstanceMap(instanceIds); // i-xxxxxxx : region

        // install and ensure NFS client is active
        SendCommandResult installNFSClientResult = installNFSClientCommand(instanceIds);
        String commandId = installNFSClientResult.getCommand().getCommandId();
        boolean isNFSInstallSuccessful = isNFSInstallSuccess(commandId);

        if (isNFSInstallSuccessful) {
            System.out.println("systems are go.");
        }
    }

    private static AWSSimpleSystemsManagement getSSMClient() {
        return AWSSimpleSystemsManagementClient.builder()
                .withRegion(Regions.US_WEST_2)
                .build();
    }

    private static AmazonEC2 getEC2Client() {
        return AmazonEC2Client.builder()
                .withRegion(Regions.US_WEST_2)
                .build();
    }

    private static AmazonElasticFileSystem getEFSClient() {
        return AmazonElasticFileSystemClient.builder()
                .withRegion(Regions.US_WEST_2)
                .build();
    }

    private static List<String> makeListOfInstanceIds(String[] args) {
        List<String> instanceIds = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            instanceIds.add(args[i]);
            System.out.println(args[i]);
        }
        System.out.println(instanceIds);
        return instanceIds;
    }

    private static Map<String, String> getInstanceMap(List<String> instanceIds) {
        AmazonEC2 amazonEC2 = getEC2Client();
        Map<String, String> instanceMap = new HashMap<>();
        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest()
                .withInstanceIds(instanceIds);
        DescribeInstancesResult describeInstancesResult = amazonEC2.describeInstances(describeInstancesRequest);

        for (Reservation reservation : describeInstancesResult.getReservations()) {
            for (Instance instance : reservation.getInstances()) {
                instanceMap.put(instance.getInstanceId(), instance.getPlacement().getAvailabilityZone());
            }
        }
        amazonEC2.shutdown();
        return instanceMap;
    }

    private static Map<String, String> getCustomEFSInfo(String fileSystemName) {
        AmazonElasticFileSystem amazonEFS = getEFSClient();
        Map<String, String> fileSystemInfo = new HashMap<>();
        DescribeFileSystemsResult fileSystemDescriptionResult = amazonEFS.describeFileSystems();
        List<FileSystemDescription> fileSystemDescriptions = fileSystemDescriptionResult.getFileSystems();
        for (FileSystemDescription fileSystemDescription : fileSystemDescriptions) {
            if (fileSystemDescription.getName().equals(fileSystemName)) {
                fileSystemInfo.put("name", fileSystemDescription.getName());
                fileSystemInfo.put("fileSystemArn", fileSystemDescription.getFileSystemArn());
                fileSystemInfo.put("fileSystemId", fileSystemDescription.getFileSystemId());
                fileSystemInfo.put("numberOfMountTargets", String.valueOf(fileSystemDescription.getNumberOfMountTargets()));
            }
        }
        amazonEFS.shutdown();
        return fileSystemInfo;
    }

    private static List<MountTargetDescription> mountTargetDescriptions(Map<String, String> fileSystemInfo) {
        AmazonElasticFileSystem amazonEFS = getEFSClient();
        DescribeMountTargetsRequest mountTargetsRequest = new DescribeMountTargetsRequest()
                .withFileSystemId(fileSystemInfo.get("fileSystemId"));
        DescribeMountTargetsResult mountTargetsResult = amazonEFS.describeMountTargets(mountTargetsRequest);
        amazonEFS.shutdown();
        return mountTargetsResult.getMountTargets();
    }

    // example: "us-west-2a" : "xxx.xx.xxx.xxx"
    private static Map<String, String> getMountTargetsDetails(List<MountTargetDescription> targetDescriptions) {
        Map<String, String> targets = new HashMap<>();
        for (MountTargetDescription description : targetDescriptions) {
            targets.put(description.getAvailabilityZoneName(), description.getIpAddress());
        }
        return targets;
    }

    private static SendCommandResult installNFSClientCommand(List<String> instanceIds) {
        AWSSimpleSystemsManagement amazonSSM = getSSMClient();
        String installCommand = "sudo yum -y install nfs-utils";
        List<String> commands = new ArrayList<>();
        commands.add(installCommand);
        Map<String, List<String>> params = new HashMap<>();

        params.put("commands", commands);

        SendCommandRequest sendCommandRequest = new SendCommandRequest()
                .withInstanceIds(instanceIds)
                .withDocumentName("AWS-RunShellScript")
                .withParameters(params);

        return amazonSSM.sendCommand(sendCommandRequest);
    }

    private static boolean isNFSInstallSuccess(String commandId) throws InterruptedException {
        boolean isSuccess = true;
        ListCommandInvocationsRequest commandInvocationsRequest = new ListCommandInvocationsRequest()
                .withCommandId(commandId)
                .withDetails(true);

        // let's sleep, so we have time to process the command
        System.out.println("processing requests");
        TimeUnit.MINUTES.sleep(3);

        ListCommandInvocationsResult commandInvocationsResult = getSSMClient().listCommandInvocations(commandInvocationsRequest);
        List<CommandInvocation> invocations = commandInvocationsResult.getCommandInvocations();
        for (CommandInvocation invocation : invocations) {
            String status = invocation.getStatus();

            System.out.printf("install nfs-client on instance: %s is '%s'%n", invocation.getInstanceId(), status);
            System.out.println(invocation.getCommandPlugins().get(0).getOutput());

            // break on any unsuccessful status
            if (!status.equals("Success")) {
                isSuccess = false;
            }
        }
        return isSuccess;
    }
}
