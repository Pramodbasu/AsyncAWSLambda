package com.pramod.aws.lambda.s3sns.api;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pramod.aws.lambda.s3sns.PatientEvent;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


//will be triggered by s3 on adding a new object
public class PatientCheckoutEventFromS3 {

    AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
  AmazonSNS sns = AmazonSNSClientBuilder.defaultClient();
  // aws context logger that why need context
  public void handler(S3Event event, Context context) throws InterruptedException {
      Thread.sleep(100);
      LambdaLogger logger = context.getLogger();
        ObjectMapper mapper = new ObjectMapper();

        event.getRecords().stream()
                .forEach(eventObj -> {

                    S3ObjectInputStream bucketObjectContentStream = s3.getObject(
                            eventObj.getS3().getBucket().getName(),
                            eventObj.getS3().getObject().getKey()
                            )
                            .getObjectContent();

                    logger.log("EV NAME " + eventObj.getEventName());
                    logger.log("EV param " + eventObj.getRequestParameters());
                    logger.log("EV gets3 " + eventObj.getS3());
                    logger.log("EV gets3bucket " + eventObj.getS3().getBucket());
                    logger.log("EV gets3 object  " + eventObj.getS3().getObject());
                    logger.log("EV source " + eventObj.getEventSource());
                    logger.log("EV userident " + eventObj.getUserIdentity());

                    try {
                        List<PatientEvent> patientCheckoutMessageList =
                                Arrays.asList(
                                        mapper.readValue(
                                                bucketObjectContentStream,
                                                PatientEvent[].class)
                                );
                        logger.log(patientCheckoutMessageList.toString());
                        patientCheckoutMessageList.stream()
                                .forEach(patientmessage->{
                                    try {
                                        sns.publish(
                                                System.getenv("PATIENT_CHECKOUT_TOPIC"),
                                              mapper.writeValueAsString(  patientmessage ));
                                    } catch (JsonProcessingException e) {
                                        throw new RuntimeException(e);
                                    }



                                });
                    } catch(JsonMappingException mappingException)
                    {
                        logger.log("Mappingt excepting with message "+mappingException.getMessage().toString());
                    }


                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

    }
}
