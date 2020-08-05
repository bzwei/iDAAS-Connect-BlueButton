/*
 * Copyright 2019 Red Hat, Inc.
 * <p>
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */
package com.redhat.idaas.connect.bluebutton;

import ca.uhn.fhir.store.IAuditDataStore;
import org.apache.camel.ExchangePattern;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.hl7.HL7;
import org.apache.camel.component.hl7.HL7MLLPNettyDecoderFactory;
import org.apache.camel.component.hl7.HL7MLLPNettyEncoderFactory;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
//import org.springframework.jms.connection.JmsTransactionManager;
//import javax.jms.ConnectionFactory;
import org.springframework.stereotype.Component;
import sun.util.calendar.BaseCalendar;
import java.time.LocalDate;

@Component
public class CamelConfiguration extends RouteBuilder {
  private static final Logger log = LoggerFactory.getLogger(CamelConfiguration.class);


  @Bean
  private KafkaEndpoint kafkaEndpoint(){
    KafkaEndpoint kafkaEndpoint = new KafkaEndpoint();
    return kafkaEndpoint;
  }
  @Bean
  private KafkaComponent kafkaComponent(KafkaEndpoint kafkaEndpoint){
    KafkaComponent kafka = new KafkaComponent();
    return kafka;
  }

  /*
   * Kafka implementation based upon https://camel.apache.org/components/latest/kafka-component.html
   *
   */
  @Override
  public void configure() throws Exception {

    /*
     * Audit
     *
     * Direct component within platform to ensure we can centralize logic
     * There are some values we will need to set within every route
     * We are doing this to ensure we dont need to build a series of beans
     * and we keep the processing as lightweight as possible
     *
     */
    from("direct:auditing")
        .setHeader("messageprocesseddate").simple("${date:now:yyyy-MM-dd}")
        .setHeader("messageprocessedtime").simple("${date:now:HH:mm:ss:SSS}")
        .setHeader("processingtype").exchangeProperty("processingtype")
        .setHeader("industrystd").exchangeProperty("industrystd")
        .setHeader("component").exchangeProperty("componentname")
        .setHeader("messagetrigger").exchangeProperty("messagetrigger")
        .setHeader("processname").exchangeProperty("processname")
        .setHeader("auditdetails").exchangeProperty("auditdetails")
        .setHeader("camelID").exchangeProperty("camelID")
        .setHeader("exchangeID").exchangeProperty("exchangeID")
        .setHeader("internalMsgID").exchangeProperty("internalMsgID")
        .setHeader("bodyData").exchangeProperty("bodyData")
        .convertBodyTo(String.class).to("kafka://localhost:9092?topic=opsmgmt_platformtransactions&brokers=localhost:9092")
    ;
    /*
    *  Logging
    */
    from("direct:logging")
        .log(LoggingLevel.INFO, log, "HL7 Admissions Message: [${body}]")
        //To invoke Logging
        //.to("direct:logging")
    ;

    /*
	 *
	 * HL7 v2x Server Implementations
	 *  ------------------------------
	 *  HL7 implementation based upon https://camel.apache.org/components/latest/dataformats/hl7-dataformat.html
	 *  For leveraging HL7 based files:
	 *  from("file:src/data-in/hl7v2/adt?delete=true?noop=true")
	 *
     *   Simple language reference
     *   https://camel.apache.org/components/latest/languages/simple-language.html
     *
     */
	  // ADT
	  from("netty4:tcp://0.0.0.0:10001?sync=true&decoder=#hl7Decoder&encoder=#hl7Encoder")
          .routeId("hl7Admissions")
          .convertBodyTo(String.class)
          // set Auditing Properties
          .setProperty("processingtype").constant("data")
          .setProperty("appname").constant("iDAAS-ConnectClinical-IndustryStd")
          .setProperty("industrystd").constant("HL7")
          .setProperty("messagetrigger").constant("ADT")
          .setProperty("componentname").simple("${routeId}")
          .setProperty("processname").constant("Input")
          .setProperty("camelID").simple("${camelId}")
          .setProperty("exchangeID").simple("${exchangeId}")
          .setProperty("internalMsgID").simple("${id}")
          .setProperty("bodyData").simple("${body}")
          .setProperty("auditdetails").constant("ADT message received")
          // iDAAS DataHub Processing
          .wireTap("direct:auditing")
          // Send to Topic
          .convertBodyTo(String.class).to("kafka://localhost:9092?topic=mctn_mms_adt&brokers=localhost:9092")
          //Response to HL7 Message Sent Built by platform
          .transform(HL7.ack())
          // This would enable persistence of the ACK
          .convertBodyTo(String.class)
          .setProperty("bodyData").simple("${body}")
          .setProperty("processingtype").constant("data")
          .setProperty("appname").constant("iDAAS-ConnectClinical-IndustryStd")
          .setProperty("industrystd").constant("HL7")
          .setProperty("messagetrigger").constant("ADT")
          .setProperty("componentname").simple("${routeId}")
          .setProperty("camelID").simple("${camelId}")
          .setProperty("exchangeID").simple("${exchangeId}")
          .setProperty("internalMsgID").simple("${id}")
          .setProperty("processname").constant("Input")
          .setProperty("auditdetails").constant("ACK Processed")
          // iDAAS DataHub Processing
          .wireTap("direct:auditing")

    ;

    // ORM
    from("netty4:tcp://0.0.0.0:10002?sync=true&decoder=#hl7Decoder&encoder=#hl7Encoder")
        .routeId("hl7Orders")
        .convertBodyTo(String.class)
        // set Auditing Properties
        .setProperty("processingtype").constant("data")
        .setProperty("appname").constant("iDAAS-ConnectClinical-IndustryStd")
        .setProperty("industrystd").constant("HL7")
        .setProperty("messagetrigger").constant("ORM")
        .setProperty("component").simple("${routeId}")
        .setProperty("processname").constant("Input")
        .setProperty("camelID").simple("${camelId}")
        .setProperty("exchangeID").simple("${exchangeId}")
        .setProperty("internalMsgID").simple("${id}")
        .setProperty("bodyData").simple("${body}")
        .setProperty("auditdetails").constant("ORM message received")
        // iDAAS DataHub Processing
        .wireTap("direct:auditing")
        // Send to Topic
        .convertBodyTo(String.class).to("kafka://localhost:9092?topic=mctn_mms_orm&brokers=localhost:9092")
        //Response to HL7 Message Sent Built by platform
        .transform(HL7.ack())
        // This would enable persistence of the ACK
        .convertBodyTo(String.class)
        .setProperty("processingtype").constant("data")
        .setProperty("appname").constant("iDAAS-ConnectClinical-IndustryStd")
        .setProperty("industrystd").constant("HL7")
        .setProperty("messagetrigger").constant("ORM")
        .setProperty("componentname").simple("${routeId}")
        .setProperty("camelID").simple("${camelId}")
        .setProperty("exchangeID").simple("${exchangeId}")
        .setProperty("internalMsgID").simple("${id}")
        .setProperty("bodyData").simple("${body}")
        .setProperty("processname").constant("Input")
        .setProperty("auditdetails").constant("ACK Processed")
        // iDAAS DataHub Processing
        .wireTap("direct:auditing")
    ;

    // ORU
    from("netty4:tcp://0.0.0.0:10003?sync=true&decoder=#hl7Decoder&encoder=#hl7Encoder")
        .routeId("hl7Results")
        .convertBodyTo(String.class)
        // set Auditing Properties
        .setProperty("processingtype").constant("data")
        .setProperty("appname").constant("iDAAS-ConnectClinical-IndustryStd")
        .setProperty("industrystd").constant("HL7")
        .setProperty("messagetrigger").constant("ORU")
        .setProperty("componentname").simple("${routeId}")
        .setProperty("camelID").simple("${camelId}")
        .setProperty("exchangeID").simple("${exchangeId}")
        .setProperty("bodyData").simple("${body}")
        .setProperty("processname").constant("Input")
        .setProperty("auditdetails").constant("ORU message received")
        // iDAAS DataHub Processing
        .wireTap("direct:auditing")
        // Send to Topic
        .convertBodyTo(String.class).to("kafka://localhost:9092?topic=mctn_mms_oru&brokers=localhost:9092")
        //Response to HL7 Message Sent Built by platform
        .transform(HL7.ack())
        // This would enable persistence of the ACK
        .convertBodyTo(String.class)
        .setProperty("processingtype").constant("data")
        .setProperty("appname").constant("iDAAS-ConnectClinical-IndustryStd")
        .setProperty("industrystd").constant("HL7")
        .setProperty("messagetrigger").constant("ORU")
        .setProperty("componentname").simple("${routeId}")
        .setProperty("camelID").simple("${camelId}")
        .setProperty("exchangeID").simple("${exchangeId}")
        .setProperty("internalMsgID").simple("${id}")
        .setProperty("bodyData").simple("${body}")
        .setProperty("processname").constant("Input")
        .setProperty("auditdetails").constant("ACK Processed")
        // iDAAS DataHub Processing
        .wireTap("direct:auditing")
    ;

    // RDE
    from("netty4:tcp://0.0.0.0:10004?sync=true&decoder=#hl7Decoder&encoder=#hl7Encoder")
        .routeId("hl7Pharmacy")
        .convertBodyTo(String.class)
        // set Auditing Properties
        .setProperty("processingtype").constant("data")
        .setProperty("appname").constant("iDAAS-ConnectClinical-IndustryStd")
        .setProperty("industrystd").constant("HL7")
        .setProperty("messagetrigger").constant("RDE")
        .setProperty("component").simple("${routeId}")
        .setProperty("camelID").simple("${camelId}")
        .setProperty("exchangeID").simple("${exchangeId}")
        .setProperty("bodyData").simple("${body}")
        .setProperty("processname").constant("Input")
        .setProperty("auditdetails").constant("RDE message received")
        // iDAAS DataHub Processing
        .wireTap("direct:auditing")
        // Send to Topic
        .convertBodyTo(String.class).to("kafka://localhost:9092?topic=mctn_mms_rde&brokers=localhost:9092")
        //Response to HL7 Message Sent Built by platform
        .transform(HL7.ack())
        // This would enable persistence of the ACK
        .convertBodyTo(String.class)
        .setProperty("processingtype").constant("data")
        .setProperty("appname").constant("iDAAS-ConnectClinical-IndustryStd")
        .setProperty("industrystd").constant("HL7")
        .setProperty("messagetrigger").constant("RDE")
        .setProperty("componentname").simple("${routeId}")
        .setProperty("camelID").simple("${camelId}")
        .setProperty("exchangeID").simple("${exchangeId}")
        .setProperty("internalMsgID").simple("${id}")
        .setProperty("bodyData").simple("${body}")
        .setProperty("processname").constant("Input")
        .setProperty("auditdetails").constant("ACK Processed")
        // iDAAS DataHub Processing
        .wireTap("direct:auditing")
    ;

    // MFN
    from("netty4:tcp://0.0.0.0:10005?sync=true&decoder=#hl7Decoder&encoder=#hl7Encoder")
        .routeId("hl7MasterFiles")
        .convertBodyTo(String.class)
        // set Auditing Properties
        .setProperty("processingtype").constant("data")
        .setProperty("appname").constant("iDAAS-ConnectClinical-IndustryStd")
        .setProperty("industrystd").constant("HL7")
        .setProperty("messagetrigger").constant("MFN")
        .setProperty("component").simple("{$routeId}")
        .setProperty("camelID").simple("${camelId}")
        .setProperty("exchangeID").simple("${exchangeId}")
        .setProperty("internalMsgID").simple("${id}")
        .setProperty("bodyData").simple("${body}")
        .setProperty("processname").constant("Input")
        .setProperty("auditdetails").constant("MFN message received")
        // iDAAS DataHub Processing
        .wireTap("direct:auditing")
        // Send to Topic
        .convertBodyTo(String.class).to("kafka://localhost:9092?topic=mctn_mms_mfn&brokers=localhost:9092")
        //Response to HL7 Message Sent Built by platform
        .transform(HL7.ack())
        // This would enable persistence of the ACK
        .convertBodyTo(String.class)
        .setProperty("processingtype").constant("data")
        .setProperty("appname").constant("iDAAS-ConnectClinical-IndustryStd")
        .setProperty("industrystd").constant("HL7")
        .setProperty("messagetrigger").constant("MFN")
        .setProperty("componentname").simple("${routeId}")
        .setProperty("camelID").simple("${camelId}")
        .setProperty("exchangeID").simple("${exchangeId}")
        .setProperty("internalMsgID").simple("${id}")
        .setProperty("bodyData").simple("${body}")
        .setProperty("processname").constant("Input")
        .setProperty("auditdetails").constant("ACK Processed")
        // iDAAS DataHub Processing
        .wireTap("direct:auditing")
    ;

    // MDM
    from("netty4:tcp://0.0.0.0:10006?sync=true&decoder=#hl7Decoder&encoder=#hl7Encoder")
         .routeId("hl7MasterDocs")
         .convertBodyTo(String.class)
         // set Auditing Properties
         .setProperty("processingtype").constant("data")
         .setProperty("appname").constant("iDAAS-ConnectClinical-IndustryStd")
         .setProperty("industrystd").constant("HL7")
         .setProperty("messagetrigger").constant("MDM")
         .setProperty("component").simple("${routeId}")
         .setProperty("camelID").simple("${camelId}")
         .setProperty("exchangeID").simple("${exchangeId}")
         .setProperty("internalMsgID").simple("${id}")
         .setProperty("bodyData").simple("${body}")
         .setProperty("processname").constant("Input")
         .setProperty("auditdetails").constant("MDM message received")
         // iDAAS DataHub Processing
         .wireTap("direct:auditing")
         //Send To Topic
         .convertBodyTo(String.class).to("kafka://localhost:9092?topic=mctn_mms_mdm&brokers=localhost:9092")
         //Response to HL7 Message Sent Built by platform
         .transform(HL7.ack())
         // This would enable persistence of the ACK
         .convertBodyTo(String.class)
         .setProperty("processingtype").constant("data")
         .setProperty("appname").constant("iDAAS-ConnectClinical-IndustryStd")
         .setProperty("industrystd").constant("HL7")
         .setProperty("messagetrigger").constant("MDM")
         .setProperty("componentname").simple("${routeId}")
         .setProperty("camelID").simple("${camelId}")
         .setProperty("exchangeID").simple("${exchangeId}")
         .setProperty("internalMsgID").simple("${id}")
         .setProperty("bodyData").simple("${body}")
         .setProperty("processname").constant("Input")
         .setProperty("auditdetails").constant("ACK Processed")
         // iDAAS DataHub Processing
         .wireTap("direct:auditing")
    ;

    // SCH
    from("netty4:tcp://0.0.0.0:10007?sync=true&decoder=#hl7Decoder&encoder=#hl7Encoder")
        .routeId("hl7Schedule")
        .convertBodyTo(String.class)
        // set Auditing Properties
        .setProperty("processingtype").constant("data")
        .setProperty("appname").constant("iDAAS-ConnectClinical-IndustryStd")
        .setProperty("industrystd").constant("HL7")
        .setProperty("messagetrigger").constant("SCH")
        .setProperty("component").simple("${routeId}")
        .setProperty("camelID").simple("${camelId}")
        .setProperty("exchangeID").simple("${exchangeId}")
        .setProperty("internalMsgID").simple("${id}")
        .setProperty("bodyData").simple("${body}")
        .setProperty("processname").constant("Input")
        .setProperty("auditdetails").constant("SCH message received")
        // iDAAS DataHub Processing
        .wireTap("direct:auditing")
        // Send To Topic
        .convertBodyTo(String.class).to("kafka://localhost:9092?topic=mctn_mms_sch&brokers=localhost:9092")
        //Response to HL7 Message Sent Built by platform
        .transform(HL7.ack())
        // This would enable persistence of the ACK
        .convertBodyTo(String.class)
        .setProperty("processingtype").constant("data")
        .setProperty("appname").constant("iDAAS-ConnectClinical-IndustryStd")
        .setProperty("industrystd").constant("HL7")
        .setProperty("messagetrigger").constant("SCH")
        .setProperty("componentname").simple("${routeId}")
        .setProperty("camelID").simple("${camelId}")
        .setProperty("exchangeID").simple("${exchangeId}")
        .setProperty("internalMsgID").simple("${id}")
        .setProperty("bodyData").simple("${body}")
        .setProperty("processname").constant("Input")
        .setProperty("auditdetails").constant("ACK Processed")
        // iDAAS DataHub Processing
        .wireTap("direct:auditing")
    ;

    // VXU
    from("netty4:tcp://0.0.0.0:10008?sync=true&decoder=#hl7Decoder&encoder=#hl7Encoder")
        .routeId("hl7Vaccination")
        .convertBodyTo(String.class)
        // set Auditing Properties
        .setProperty("processingtype").constant("data")
        .setProperty("appname").constant("iDAAS-ConnectClinical-IndustryStd")
        .setProperty("industrystd").constant("HL7")
        .setProperty("messagetrigger").constant("VXU")
        .setProperty("component").simple("${routeId}")
        .setProperty("camelID").simple("${camelId}")
        .setProperty("exchangeID").simple("${exchangeId}")
        .setProperty("internalMsgID").simple("${id}")
        .setProperty("bodyData").simple("${body}")
        .setProperty("processname").constant("Input")
        .setProperty("auditdetails").constant("VXU message received")
        // iDAAS DataHub Processing
        .wireTap("direct:auditing")
        // Send To Topic
        .convertBodyTo(String.class).to("kafka://localhost:9092?topic=mctn_mms_vxu&brokers=localhost:9092")
        //Response to HL7 Message Sent Built by platform
        .transform(HL7.ack())
        // This would enable persistence of the ACK
        .convertBodyTo(String.class)
        .setProperty("processingtype").constant("data")
        .setProperty("appname").constant("iDAAS-ConnectClinical-IndustryStd")
        .setProperty("industrystd").constant("HL7")
        .setProperty("messagetrigger").constant("VXU")
        .setProperty("componentname").simple("${routeId}")
        .setProperty("camelID").simple("${camelId}")
        .setProperty("exchangeID").simple("${exchangeId}")
        .setProperty("internalMsgID").simple("${id}")
        .setProperty("bodyData").simple("${body}")
        .setProperty("processname").constant("Input")
        .setProperty("auditdetails").constant("ACK Processed")
        // iDAAS DataHub Processing
        .wireTap("direct:auditing")
    ;

  }
}