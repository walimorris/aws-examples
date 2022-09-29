import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;

import java.util.List;
/**
 * This program utilizes the following InstanceMetaData.java class and exposes a single instance
 * to pull instance level metadata or any data provided by the sinngle instance. 
 * 
 * Lifecycle: 
 * 1. Instantiate and start a single instance
 * 2. Thread sleeps in order to allow time for instance startup completion
 * 3. Stop the instance
 * 4. Because the instance is only stopped, it allows entity to pull metadata (you could start back up again if you wanted to).
 * 5. Terminate the instance
 */
public class App {
    public static void main( String[] args ) {
        InstanceMetaData instance = new InstanceMetaData(InstanceType.T1Micro, "ami-0ee8244746ec5d6d4");
        boolean isInstantiated = instance.instantiateAndStartInstance();
        if (isInstantiated) {
            System.out.println("Instance AMI: " + instance.getAmi());
            System.out.println("InstanceId: " + instance.getInstanceId());
            System.out.println("InstancePrivateIPAddress: " + instance.getInstancePrivateIpAddress());
            System.out.println("InstanceType: " + instance.getInstanceType());

            // wait 5 minutes before stopping instance and give time
            // for instance to complete initialization
            try {
                TimeUnit.SECONDS.sleep(300);
                instance.stopInstance();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // get some instance metadata
            Instance currentInstance = instance.getInstance();
            System.out.println("Architecture: " + currentInstance.getArchitecture());
            System.out.println("Launch Time: " + currentInstance.getLaunchTime());
            System.out.println("Root Device type: " + currentInstance.getRootDeviceType());

            // terminate instance
            instance.terminateInstance();
        }
    }
}

public class InstanceMetaData {
    private final AmazonEC2 ec2Client;
    private String instanceId;
    private final String ami;
    private final InstanceType instanceType;
    private String instancePrivateIpAddress;

    private boolean isRunning;
    private Instance instance;

    public InstanceMetaData(InstanceType instanceType, String ami) {
        this.ec2Client = AmazonEC2Client.builder()
                .withRegion(Regions.US_WEST_2)
                .build();
        this.isRunning = false;
        this.instance = null;
        this.instancePrivateIpAddress = null;
        this.instanceId = null;
        this.ami = ami == null ? "ami-0ee8244746ec5d6d4" : ami;
        this.instanceType = instanceType == null ? InstanceType.T1Micro : instanceType;
    }

    public boolean instantiateAndStartInstance() {
        if (!this.isRunning && this.instance == null) {
            try {
                RunInstancesRequest runInstancesRequest = new RunInstancesRequest(this.ami, 1, 1)
                        .withInstanceType(this.instanceType);
                
                RunInstancesResult runInstancesResult = this.ec2Client.runInstances(runInstancesRequest);

                this.instanceId = runInstancesResult.getReservation()
                    .getInstances()
                    .get(0)
                    .getInstanceId();
                
                this.instancePrivateIpAddress = runInstancesResult.getReservation()
                    .getInstances()
                    .get(0)
                    .getPrivateIpAddress();

                StartInstancesRequest startInstancesRequest = new StartInstancesRequest().withInstanceIds(this.instanceId);
                StartInstancesResult startInstancesResult = this.ec2Client.startInstances(startInstancesRequest);
                String startInstanceId = startInstancesResult.getStartingInstances()
                    .get(0)
                    .getInstanceId();
                
                if (startInstanceId.equals(this.instanceId)) {
                    this.isRunning = true;
                    this.instance = describeInstance(this.instanceId);
                }
            } catch (Exception e) {
                terminateInstance();
                System.out.println(e.getMessage());
            }
        }
        return this.isRunning;
    }

    public void stopInstance() {
        if (this.isRunning) {
            StopInstancesRequest stopInstancesRequest = new StopInstancesRequest().withInstanceIds(this.instanceId);
            StopInstancesResult stopInstancesResult = this.ec2Client.stopInstances(stopInstancesRequest);
            String stoppedInstanceId = stopInstancesResult.getStoppingInstances()
                .get(0)
                .getInstanceId();
            
            if (stoppedInstanceId.equals(this.instanceId)) {
                System.out.printf("stopping instance with id: '%s'%n", stoppedInstanceId);
                this.isRunning = false;
            }
        } else {
            System.out.println("Instance is not running");
        }
    }

    /**
     * States:
     * custom/ 1 : unknown
     * 0 : pending
     * 16 : running
     * 32 : shutting down
     * 48 : terminated
     * 64: stopping
     * 80 : stopped
     */
    public void terminateInstance() {
        int code = getInstateStateCode();
        System.out.println("code: " + code);

        // stopping or stopped
        if (code != 48) {
            TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest().withInstanceIds(this.instanceId);
            TerminateInstancesResult terminateInstancesResult = this.ec2Client.terminateInstances(terminateInstancesRequest);
            String terminatedInstanceId = terminateInstancesResult.getTerminatingInstances()
                .get(0)
                .getInstanceId();
            
            if (terminatedInstanceId.equals(this.instanceId)) {
                System.out.printf("terminating instance with id: '%s'%n", terminatedInstanceId);
                this.isRunning = false;
            }
        } else {
            System.out.println("Instance is not running");
        }
    }

    public String getInstanceId() {
        return this.instanceId;
    }

    public String getInstancePrivateIpAddress() {
        return this.instancePrivateIpAddress;
    }

    public String getAmi() {
        return this.ami;
    }

    public InstanceType getInstanceType() {
        return this.instanceType;
    }

    public Instance getInstance() {
        return this.instance;
    }

    private Instance describeInstance(String instanceId) {
        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest()
                .withInstanceIds(instanceId);
        DescribeInstancesResult describeInstancesResult = this.ec2Client.describeInstances(describeInstancesRequest);
        List<Reservation> reservations = describeInstancesResult.getReservations();
        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();
            for (Instance instance : instances) {
                if (instance.getInstanceId().equals(this.instanceId)) {
                    this.instance = instance;
                    break;
                }
            }
            if (this.instance != null) {
                break;
            }
        }
        return this.instance;
    }

    private int getInstateStateCode() {
        if (this.instance != null) {
            return this.instance.getState().getCode();
        }
        return 1;
    }
}
