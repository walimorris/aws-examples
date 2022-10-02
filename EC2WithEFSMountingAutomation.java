import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.elasticfilesystem.AmazonElasticFileSystem;
import com.amazonaws.services.elasticfilesystem.AmazonElasticFileSystemClient;
import com.amazonaws.services.elasticfilesystem.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class App {
    public static void main( String[] args ) {
        String fileSystemName = "xxx"; // args[0]
//        List<String> addresses = new ArrayList<>(Arrays.asList(args).subList(1, args.length));
        Map<String, String> efs = getCustomEFSInfo(fileSystemName);
        List<MountTargetDescription> targetDescriptions = mountTargetDescriptions(efs);
        Map<String, String> mountTargets = getMountTargetsDetails(targetDescriptions);
        Map<String, String> instanceMap = getInstanceMap("xxxxxxxxx"); // will be a list of instance ids

        // now i figure out how to mount 
        
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

    private static Map<String, String> getInstanceMap(String instanceId) {
        Map<String, String> instanceMap = new HashMap<>();
        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest()
                .withInstanceIds(instanceId);
        DescribeInstancesResult describeInstancesResult = getEC2Client().describeInstances(describeInstancesRequest);

        List<Instance> instances = describeInstancesResult.getReservations().get(0).getInstances();
        for (Instance instance : instances) {
            instanceMap.put("instanceId", instance.getInstanceId());
            instanceMap.put("availabilityZone", instance.getPlacement().getAvailabilityZone());
        }
        return instanceMap;
    }

    private static Map<String, String> getCustomEFSInfo(String fileSystemName) {
        Map<String, String> fileSystemInfo = new HashMap<>();
        DescribeFileSystemsResult fileSystemDescriptionResult = getEFSClient().describeFileSystems();
        List<FileSystemDescription> fileSystemDescriptions = fileSystemDescriptionResult.getFileSystems();
        for (FileSystemDescription fileSystemDescription : fileSystemDescriptions) {
            if (fileSystemDescription.getName().equals(fileSystemName)) {
                fileSystemInfo.put("name", fileSystemDescription.getName());
                fileSystemInfo.put("fileSystemArn", fileSystemDescription.getFileSystemArn());
                fileSystemInfo.put("fileSystemId", fileSystemDescription.getFileSystemId());
                fileSystemInfo.put("numberOfMountTargets", String.valueOf(fileSystemDescription.getNumberOfMountTargets()));
            }
        }
        return fileSystemInfo;
    }

    private static List<MountTargetDescription> mountTargetDescriptions(Map<String, String> fileSystemInfo) {
        DescribeMountTargetsRequest mountTargetsRequest = new DescribeMountTargetsRequest()
                .withFileSystemId(fileSystemInfo.get("fileSystemId"));
        DescribeMountTargetsResult mountTargetsResult = getEFSClient().describeMountTargets(mountTargetsRequest);
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
}
