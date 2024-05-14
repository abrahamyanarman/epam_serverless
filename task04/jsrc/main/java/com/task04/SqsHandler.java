package com.task04;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.syndicate.deployment.annotations.events.SqsTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@LambdaHandler(lambdaName = "sqs_handler",
	roleName = "sqs_handler-role",
	isPublishVersion = false,
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@SqsTriggerEventSource(targetQueue = "async_queue", batchSize = 1)
public class SqsHandler implements RequestHandler<SQSEvent, Void> {
	private static final Logger logger = Logger.getLogger(SqsHandler.class.getName());
	public Void handleRequest(SQSEvent event, Context context) {
		logger.info("SqsHandler: Received event: " + event);

		try {
			List<SQSMessage> records = event.getRecords();
			for (SQSMessage message : records) {
				String messageBody = message.getBody();
				logger.info("SqsHandler: Received message: " + messageBody);
			}
		} catch (Exception e) {
			logger.severe("Error handling SQS event: " + e.getMessage());
		}
		return null;
	}
}
