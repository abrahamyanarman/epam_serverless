package com.task05;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.google.gson.Gson;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;
import com.task05.model.Event;
import com.task05.model.RequestBody;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(lambdaName = "api_handler",
	roleName = "api_handler-role",
	isPublishVersion = false,
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED,
		timeout = 60
)
public class ApiHandler implements RequestHandler<RequestBody, APIGatewayProxyResponseEvent> {

	private static final String TABLE_NAME = "cmtr-a7a5b08f-Events-test";

	private final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
	private final DynamoDB dynamoDB = new DynamoDB(client);

	public APIGatewayProxyResponseEvent handleRequest(RequestBody request, Context context) {
		int principalId = request.getPrincipalId();
		Map<String, String> content = request.getContent();

		String eventId = UUID.randomUUID().toString();

		String createdAt = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);

		Event event = new Event();
		event.setId(eventId);
		event.setPrincipalId(principalId);
		event.setCreatedAt(createdAt);
		event.setBody(content);

		String eventData = new Gson().toJson(event);

		saveEventToDynamoDB(eventData);

		APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
		response.setStatusCode(201);
		response.setBody(eventData);

		return response;
	}

	private void saveEventToDynamoDB(String eventData) {
		Table table = dynamoDB.getTable(TABLE_NAME);
		Item item = new Item().withPrimaryKey("Id", UUID.randomUUID().toString())
				.withString("EventData", eventData);
		PutItemOutcome outcome = table.putItem(item);
	}
}
