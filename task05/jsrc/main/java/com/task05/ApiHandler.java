package com.task05;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.ResourceType;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(
		lambdaName = "api_handler",
		roleName = "api_handler-role",
		isPublishVersion = true,
		aliasName = "${lambdas_alias_name}",
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@LambdaUrlConfig(
		authType = AuthType.NONE,
		invokeMode = InvokeMode.BUFFERED
)
@DependsOn(name = "Events", resourceType = ResourceType.DYNAMODB_TABLE)
@EnvironmentVariables({
		@EnvironmentVariable(key = "region", value = "${region}"),
		@EnvironmentVariable(key = "table", value = "${target_table}")
})
public class ApiHandler implements RequestHandler<Object, APIGatewayV2HTTPResponse> {

	private static final ObjectMapper objectMapper = new ObjectMapper();
	private static final AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();

	@Override
	public APIGatewayV2HTTPResponse handleRequest(Object event, Context context) {
		LambdaLogger logger = context.getLogger();
		try {
			logger.log("Received event: " + objectMapper.writeValueAsString(event));

			Map<String, Object> requestData = objectMapper.readValue(objectMapper.writeValueAsString(event), LinkedHashMap.class);

			if (!requestData.containsKey("body")) {
				throw new IllegalArgumentException("Request body is missing");
			}

			Map<String, Object> body = objectMapper.readValue((String) requestData.get("body"), LinkedHashMap.class);

			if (!body.containsKey("principalId") || !body.containsKey("content")) {
				throw new IllegalArgumentException("Required fields are missing in request body");
			}

			int principalId = Integer.parseInt(body.get("principalId").toString());
			Map<String, String> content = objectMapper.convertValue(body.get("content"), Map.class);

			String eventId = UUID.randomUUID().toString();
			String createdAt = Instant.now().toString();

			Item item = new Item()
					.withString("id", eventId)
					.withInt("principalId", principalId)
					.withString("createdAt", createdAt)
					.withMap("body", content);

			PutItemRequest putRequest = new PutItemRequest()
					.withTableName(System.getenv("table"))
					.withItem(ItemUtils.toAttributeValues(item));

			PutItemResult putResult = dynamoDB.putItem(putRequest);
			logger.log("DynamoDB PutItem Result: " + putResult);

			Map<String, Object> responseBody = new HashMap<>();
			responseBody.put("id", eventId);
			responseBody.put("principalId", principalId);
			responseBody.put("createdAt", createdAt);
			responseBody.put("body", content);

			return APIGatewayV2HTTPResponse.builder()
					.withStatusCode(201)
					.withHeaders(Map.of("Content-Type", "application/json"))
					.withBody(objectMapper.writeValueAsString(Map.of("statusCode", 201, "event", responseBody)))
					.withIsBase64Encoded(false)
					.build();

		} catch (Exception e) {
			logger.log("Error encountered: " + e.getMessage());
			return APIGatewayV2HTTPResponse.builder()
					.withStatusCode(500)
					.withHeaders(Map.of("Content-Type", "application/json"))
					.withBody("{\"message\": \"Internal server error\"}")
					.withIsBase64Encoded(false)
					.build();
		}
	}
}
