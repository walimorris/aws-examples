package org.temp;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;

import java.util.List;
import java.util.Map;

/**
 * Example using DynamoDB Streams and Lambda for processing some data change on DDB table items.
 * In this example we have a DDB table modeling a thing (raspberry pi) that contains a composite
 * primary key (#hash=machineId, #range=machineType) with a attribute of temperature. This example
 * demonstrates a things temperature being updated in a DDB table. DDB Streams is triggered on
 * temperature updates and records the new data points in the stream. This triggers a lambda function
 * which then reports the temperature state of the thing. In a more elaborate process, this lambda
 * function could do more: send notification to technicians, shutdown a thing, start an internal fan
 * and so on. Below are the steps.
 * <P>
 *     1. Create the table with some test data, the primary key and attributes.
 *     2. Create a Lambda function with the business logic and provide attach
 *        a policy that gives the lambda function the correct permissions to
 *        interact with DDB/DDB Streams. Attach this policy to the Lambda Functions
 *        role.
 *     3. Enable DDB Streams on the table and ensure to project the NEW IMAGE.
 *        Assign the created Lambda function with correct permissions as a
 *        trigger.
 *     4. TEST
 *  <P>
 */
public class TemperatureChange {

    private static final String TEMPERATURE = "temperature";
    private static final String MACHINE_ID = "machineId";
    private static final String OVERHEATED = "OVERHEATED";
    private static final String NORMAL_STATE = "NORMAL STATE";

    public String handleRequest(DynamodbEvent event, Context context) {
        LambdaLogger logger = context.getLogger();

        List<DynamodbStreamRecord> eventRecords = event.getRecords();
        for (DynamodbStreamRecord record : eventRecords) {
            Map<String, AttributeValue> recordKeys = record.getDynamodb().getNewImage();

            recordKeys.forEach((key, value) -> {

                // value will have a trailing comma and type declaration in dynamo fashion
                // we must remove this ex - {S: 65.2,}
                String v = String.valueOf(value);
                v = v.substring(3, v.length() - 2).trim();

                if (key.equals(TEMPERATURE) && Double.parseDouble(v) > 80) {
                    logger.log(recordKeys.get(MACHINE_ID) + " state: " + OVERHEATED);
                } else {
                    if (key.equals(TEMPERATURE) && Double.parseDouble(v) < 80) {
                        logger.log(recordKeys.get(MACHINE_ID) + " state: " + NORMAL_STATE);
                    }
                }
            });
        }
        return "success";
    }
}
