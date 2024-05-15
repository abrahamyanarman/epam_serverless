package com.task04;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.SNSRecord;
import com.syndicate.deployment.annotations.events.SnsEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@LambdaHandler(lambdaName = "sns_handler",
	roleName = "sns_handler-role",
	isPublishVersion = false,
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED,
		timeout = 60
)
@SnsEventSource(targetTopic = "lambda_topic")
public class SnsHandler implements RequestHandler<SNSEvent, Void> {
	private static final Logger logger = Logger.getLogger(SnsHandler.class.getName());
	public Void handleRequest(SNSEvent event, Context context) {
		logger.info("SnsHandler: Received event: " + event);

		try {
			List<SNSRecord> records = event.getRecords();
			for (SNSRecord record : records) {
				String messageBody = record.getSNS().getMessage();
				logger.info("SnsHandler: Received message: " + messageBody);
			}
		} catch (Exception e) {
			logger.severe("Error handling SNS event: " + e.getMessage());
		}

		return null;
	}
}
