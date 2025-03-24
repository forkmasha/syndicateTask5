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
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "region", value = "${region}"),
		@EnvironmentVariable(key = "table", value = "${target_table}")})
public class ApiHandler implements RequestHandler<Object, APIGatewayV2HTTPResponse> {

	private static final ObjectMapper objectMapper = new ObjectMapper();
	private static final AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();

	@Override
	public APIGatewayV2HTTPResponse handleRequest(Object input, Context context) {
		LambdaLogger logger = context.getLogger();
		try {
			logger.log("Processing request: " + objectMapper.writeValueAsString(input));

			Map<String, Object> requestData = parseRequestBody(input, logger);

			int principalId = extractPrincipalId(requestData);
			Map<String, String> content = extractContent(requestData);

			String eventId = UUID.randomUUID().toString();
			String createdAt = Instant.now().toString();

			storeEvent(eventId, principalId, createdAt, content, logger);

			Map<String, Object> responseData = buildResponseData(eventId, principalId, createdAt, content);

			return buildSuccessResponse(responseData);

		} catch (Exception e) {
			logger.log("Error encountered: " + e.getMessage());
			return buildErrorResponse();
		}
	}

	private Map<String, Object> parseRequestBody(Object input, LambdaLogger logger) throws Exception {
		return objectMapper.readValue(objectMapper.writeValueAsString(input), HashMap.class);
	}

	private int extractPrincipalId(Map<String, Object> requestData) {
		Object principalIdObject = requestData.get("principalId");
		if (principalIdObject == null) {
			throw new IllegalArgumentException("Missing required field: principalId");
		}
		return (principalIdObject instanceof Number)
				? ((Number) principalIdObject).intValue()
				: Integer.parseInt(principalIdObject.toString());
	}

	private Map<String, String> extractContent(Map<String, Object> requestData) {
		Object contentObject = requestData.get("content");
		if (contentObject == null) {
			throw new IllegalArgumentException("Missing required field: content");
		}
		return objectMapper.convertValue(contentObject, Map.class);
	}

	private void storeEvent(String eventId, int principalId, String createdAt, Map<String, String> content, LambdaLogger logger) {
		Item eventItem = new Item()
				.withString("id", eventId)
				.withInt("principalId", principalId)
				.withString("createdAt", createdAt)
				.withMap("body", content);

		PutItemRequest putRequest = new PutItemRequest()
				.withTableName(System.getenv("table"))
				.withItem(ItemUtils.toAttributeValues(eventItem));

		PutItemResult putResult = dynamoDB.putItem(putRequest);
		logger.log("Event stored in DynamoDB: " + putResult.toString());
	}

	private Map<String, Object> buildResponseData(String eventId, int principalId, String createdAt, Map<String, String> content) {
		Map<String, Object> responseData = new HashMap<>();
		responseData.put("id", eventId);
		responseData.put("principalId", principalId);
		responseData.put("createdAt", createdAt);
		responseData.put("body", content);
		return responseData;
	}

	private APIGatewayV2HTTPResponse buildSuccessResponse(Map<String, Object> responseData) throws Exception {
		return APIGatewayV2HTTPResponse.builder()
				.withStatusCode(201)
				.withHeaders(Map.of("Content-Type", "application/json"))
				.withBody(objectMapper.writeValueAsString(Map.of("statusCode", 201, "event", responseData)))
				.withIsBase64Encoded(false)
				.build();
	}

	private APIGatewayV2HTTPResponse buildErrorResponse() {
		return APIGatewayV2HTTPResponse.builder()
				.withStatusCode(500)
				.withHeaders(Map.of("Content-Type", "application/json"))
				.withBody("{\"message\": \"Internal server error\"}")
				.withIsBase64Encoded(false)
				.build();
	}
}
