package com.pramod.aws.lambda.s3sns.errorhandling;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
public class ErrorHandler {

    public void handler(SNSEvent event , Context context)
    {

        LambdaLogger logger = context.getLogger();

        event.getRecords().stream()
                .forEach(records->{
                    logger.log("dead letter queue "+records.toString());
                });
    }
}
