import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.elasticfilesystem.AmazonElasticFileSystem;
import com.amazonaws.services.elasticfilesystem.AmazonElasticFileSystemClient;
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

        String mountTarget = args[0]; // args[0]
        List<String> instanceIds = makeListOfInstanceIds(args);

        // install and ensure NFS client is active
        SendCommandResult installNFSClientResult = installNFSClientCommand(instanceIds);
        String commandId = installNFSClientResult.getCommand().getCommandId();
        boolean isNFSInstallSuccessful = isNFSInstallSuccess(commandId);

        if (isNFSInstallSuccessful) {
            System.out.println("systems are go.");
        }

        // now we must mount each instance in the region it is in to the region for our mount target
        SendCommandResult mountFileSystemOnInstancesResult = mountFileSystemsOnInstances(mountTarget, instanceIds);
        String mountCommandId = mountFileSystemOnInstancesResult.getCommand().getCommandId();

        ListCommandInvocationsRequest commandInvocationsRequest = new ListCommandInvocationsRequest()
                .withCommandId(mountCommandId)
                .withDetails(true);

        System.out.printf("processing mount requests at target: %s%n", mountTarget);
        TimeUnit.MINUTES.sleep(5);

        AWSSimpleSystemsManagement amazonSSM = getSSMClient();
        ListCommandInvocationsResult commandInvocationsResult = amazonSSM.listCommandInvocations(commandInvocationsRequest);
        List<CommandInvocation> invocations = commandInvocationsResult.getCommandInvocations();
        for (CommandInvocation invocation : invocations) {
            String status = invocation.getStatus();

            System.out.printf("mount file system on instance: %s is '%s'%n", invocation.getInstanceId(), status);
            System.out.println(invocation.getCommandPlugins().get(0).getOutput());
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
        System.out.println("processing NFS-client install requests");
        TimeUnit.MINUTES.sleep(1);

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

    // EC2 instance can resolve mount target DNS name to the IP address :) yay AWS
    private static SendCommandResult mountFileSystemsOnInstances(String mountTargetDNS, List<String> instanceIds) {
        AWSSimpleSystemsManagement amazonSSM = getSSMClient();
        String mkdirCommand = "mkdir ~/efs-mount-point ";
        String mountFileSystem = String.format("sudo mount -t nfs -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport %s:/   ~/efs-mount-point", mountTargetDNS);
        List<String> commands = new ArrayList<>();
//        commands.add(mkdirCommand);
        commands.add(mountFileSystem);
        Map<String, List<String>> params = new HashMap<>();

        SendCommandRequest sendCommandRequest = new SendCommandRequest()
                .withInstanceIds(instanceIds)
                .withDocumentName("AWS-RunShellScript")
                .withParameters(params);

        params.put("commands", commands);
        return amazonSSM.sendCommand(sendCommandRequest);
    }
}
