package com.task06;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.syndicate.deployment.annotations.events.DynamoDbEvents;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

@LambdaHandler(lambdaName = "audit_producer",
	roleName = "audit_producer-role",
	isPublishVersion = false,
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@DynamoDbTriggerEventSource(targetTable = "Configuration", batchSize = 1)
public class AuditProducer implements RequestHandler<DynamodbEvent, Void> {

	private static final Logger logger = Logger.getLogger(AuditProducer.class.getName());
	private static final String INSERT_EVENT = "INSERT";
	private static final String MODIFY_EVENT = "MODIFY";
	private static final String TABLE_NAME = "cmtr-a7a5b08f-Audit-test";

	private final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
	private final DynamoDB dynamoDB = new DynamoDB(client);
	public Void handleRequest(DynamodbEvent dynamodbEvent, Context context) {
		logger.info("Received event: " + dynamodbEvent);
		List<DynamodbStreamRecord> records = dynamodbEvent.getRecords();
		Table table = dynamoDB.getTable(TABLE_NAME);
		for (DynamodbStreamRecord record : records) {
			if (record.getEventName().equals(INSERT_EVENT)) {
				Map<String, AttributeValue> newImage = record.getDynamodb().getNewImage();
				Item item = new Item().withPrimaryKey("id", UUID.randomUUID().toString())
						.withString("itemKey", newImage.get("key").getS())
						.withString("modificationTime", LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME))
						.withMap("newValue", Map.of("key", newImage.get("key").getS(), "value",
								Integer.parseInt(newImage.get("value").getS())));
				table.putItem(item);
			}
			if (record.getEventName().equals(MODIFY_EVENT)) {
				Map<String, AttributeValue> newImage = record.getDynamodb().getNewImage();
				Map<String, AttributeValue> oldImage = record.getDynamodb().getOldImage();
				Item item = new Item().withPrimaryKey("id", UUID.randomUUID().toString())
						.withString("itemKey", oldImage.get("key").getS())
						.withString("modificationTime", LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME))
						.withString("updatedAttribute", "value")
						.withInt("oldValue", Integer.parseInt(oldImage.get("value").getS()))
						.withInt("newValue", Integer.parseInt(newImage.get("value").getS()));
				table.putItem(item);
			}
		}
		return null;
	}
}
