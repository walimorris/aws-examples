import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Simple lambda function that can be integrated with Amazon API Gateway.
 */
public class ProxyHandler implements RequestStreamHandler {

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        JSONTokener tokener = new JSONTokener(reader);
        JSONObject responseJson = new JSONObject();

        JSONObject event = new JSONObject(tokener);
        if (!event.isEmpty()) {
            JSONObject requestContextJSON = (JSONObject) event.get("requestContext");
            JSONObject identityJSON = (JSONObject) requestContextJSON.get("identity");

            JSONObject responseBody = new JSONObject();

            responseBody.put("requestTime", requestContextJSON.get("requestTime").toString());
            responseBody.put("requestSourceIp", identityJSON.get("sourceIp").toString());
            responseBody.put("apiId", requestContextJSON.get("apiId").toString());

            responseJson.put("statusCode", 200);
            responseJson.put("body", responseBody);
        }

        OutputStreamWriter writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
        writer.write(responseJson.toString());
        writer.close();
    }
}
