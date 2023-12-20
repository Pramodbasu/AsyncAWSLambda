package com.pramod.aws.lambda.s3sns.api;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pramod.aws.lambda.s3sns.PatientEvent;


import java.util.logging.Logger;

//will be triggered by s3 on adding a new object
public class BillManagementLambda {

    ObjectMapper mapper = new ObjectMapper();
    public void handler(SNSEvent snsEvent, Context context) {

        LambdaLogger logger = context.getLogger();
        snsEvent.getRecords()
                .stream()
                .forEach(
                        snsRecord -> {
                            try {
                                System.out.println();
                                PatientEvent data = mapper.readValue(snsRecord.getSNS().getMessage(), PatientEvent.class);
                                logger.log("SNS Message <==>  " + data);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        }
                );


    }
}
