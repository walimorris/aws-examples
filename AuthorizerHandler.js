/**
 * This function handles a 'simple response' for a Lambda Authorizer function. This function will 
 * be executed as the authorizer integration for an API Gateway endpoint. This function will check
 * that the http request headers contain the secret key that will authorize api gateway to execute
 * the initial lambda function triggered from an end user hitting an api gateway endpoint. 
 * The secret key should be stored in Secrets Manager and puled into code using the Secrets Manager 
 * API. If the secret matches, our Lambda authorizer will authorize api gateway to execute the 
 * original lambda function integrated with the http endpoint.
 **/
export const handler = async(event) => {
    let response = {
        "isAuthorized": false,
        "context": {
            "extraContent": "value"
        }
    };
    
    console.log(event);
    
    if (event.headers.authorization === '******') {
        response = {
            "isAuthorized": true,
            "context": {
                "extraContent": "value"
            }
        }
    };
    return response;
};
