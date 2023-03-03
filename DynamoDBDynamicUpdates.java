package org.temp.api;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import org.temp.api.models.Streams;

import java.util.Map;
import java.util.Random;

public class SendTemperature {

    public static final String MACHINE_TYPE = "machineType";
    public static final String TEMPERATURE = "temperature";
    public static final String MACHINE_NAME = "machineName";

    public static final String MACHINE_NAME_INDEX = "machine-name-index";

    public String handleRequest(APIGatewayV2HTTPEvent event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("inside SendTemperature");

        AmazonDynamoDB amazonDynamoDB = getAmazonDynamoDBClient();

        String machineType;
        String temperature;
        String machineName;
        boolean isMachineUpdated = false;

        if (event.getQueryStringParameters() != null) {
            Map<String, String> requestParameters = event.getQueryStringParameters();
            machineName = requestParameters.get(MACHINE_NAME);
            machineType = requestParameters.get(MACHINE_TYPE);
            temperature = requestParameters.get(TEMPERATURE);

            isMachineUpdated = updateMachineAttributes(amazonDynamoDB, machineName, machineType, temperature, logger);
        } else {
            logger.log("QueryString Parameters are null");
        }
        logger.log("Machine has been added to DDB Streams table: " + isMachineUpdated);
        amazonDynamoDB.shutdown();
        return "success\n";
    }

    private int generateRandomId() {
        Random randomNumberGenerator = new Random();
        StringBuilder randomNumber = new StringBuilder();
        for (int i = 0; i < 9; i++) {
            randomNumber.append(randomNumberGenerator.nextInt(9));
        }
        return Integer.parseInt(randomNumber.toString());
    }

    /**
     * Provides configuration setting for {@link AmazonDynamoDB} and {@link AmazonDynamoDBStreams}
     * clients. Configurations come with default settings for SDK retry, error, and timeout logic,
     * as a few examples, though custom configurations provide a way to customize this SDK logic
     * based on your application needs.
     *
     * @return custom {@link ClientConfiguration}
     */
    private static ClientConfiguration dynamoClientConfiguration() {
        return new ClientConfiguration()
                .withConnectionTimeout(120)
                .withMaxErrorRetry(3)
                .withThrottledRetries(true);
    }

    private static AmazonDynamoDB getAmazonDynamoDBClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .withClientConfiguration(dynamoClientConfiguration())
                .build();
    }

    public Streams queryStreamsItemByMachineName(AmazonDynamoDB amazonDynamoDB, String machineName, LambdaLogger logger) {
        DynamoDBMapper mapper = new DynamoDBMapper(amazonDynamoDB);

        Streams streamsItem = new Streams();
        streamsItem.setMachineName(machineName);

        DynamoDBQueryExpression<Streams> queryExpression = new DynamoDBQueryExpression<Streams>()
                .withConsistentRead(false)
                .withHashKeyValues(streamsItem)
                .withIndexName(MACHINE_NAME_INDEX);

        try {
            return mapper.query(Streams.class, queryExpression).get(0);
        } catch (Exception e) {

            /*
            Return a generic Streams Object if the queried Item is not found on the machine-name-index.
            This signifies that the Item doesn't exist and allows us to create it from this empty
            Streams object
             */
            logger.log("Error querying " + machineName + " on the " + MACHINE_NAME_INDEX);
            return new Streams();
        }
    }

    public boolean updateMachineAttributes(AmazonDynamoDB amazonDynamoDB, String machineName, String machineType,
                                           String temperature, LambdaLogger logger) {

        DynamoDBMapper mapper = new DynamoDBMapper(amazonDynamoDB);
        Streams streamsItemResult = queryStreamsItemByMachineName(amazonDynamoDB, machineName, logger);

        int machineId;
        if (streamsItemResult.getMachineId() != - 1) {
            machineId = streamsItemResult.getMachineId();
        } else {
            machineId = generateRandomId();
            streamsItemResult.setMachineId(machineId);
            streamsItemResult.setMachineName(machineName);
            streamsItemResult.setMachineType(machineType);
        }
        streamsItemResult.setTemperature(temperature);
        logger.log("MachineId=" + machineId);

        try {
            mapper.save(streamsItemResult);
        } catch (Exception e) {
            logger.log("Error updating Streams Item with machine name " + machineName + ": " + e.getMessage());
            return false;
        }

        return true;
    }
}
