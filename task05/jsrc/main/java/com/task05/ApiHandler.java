package com.task05;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

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
public class ApiHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

	private static final String TABLE_NAME = "Events";
	private final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
	private final DynamoDB dynamoDB = new DynamoDB(client);
	private final ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public Map<String, Object> handleRequest(Map<String, Object> request, Context context) {
		try {

			Integer principalId = (Integer) request.get("principalId");
			Map<String, Object> content = (Map<String, Object>) request.get("content");

			if (principalId == null || content == null) {
				return createResponse(400, "Invalid request: missing principalId or content");
			}


			String id = UUID.randomUUID().toString();
			String createdAt = Instant.now().toString();


			Table table = dynamoDB.getTable(TABLE_NAME);
			Item item = new Item()
					.withPrimaryKey("id", id)
					.withNumber("principalId", principalId)
					.withString("createdAt", createdAt)
					.withMap("body", content);

			table.putItem(item);


			Map<String, Object> event = new HashMap<>();
			event.put("id", id);
			event.put("principalId", principalId);
			event.put("createdAt", createdAt);
			event.put("body", content);

			return createResponse(201, event);
		} catch (Exception e) {
			context.getLogger().log("Error: " + e.getMessage());
			return createResponse(500, "Internal server error");
		}
	}

	private Map<String, Object> createResponse(int statusCode, Object body) {
		Map<String, Object> response = new HashMap<>();
		response.put("statusCode", statusCode);
		response.put("event", body);
		return response;
	}
}
