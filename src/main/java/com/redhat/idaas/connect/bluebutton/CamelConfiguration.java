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
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.hl7.HL7;
import org.apache.camel.component.hl7.HL7MLLPNettyDecoderFactory;
import org.apache.camel.component.hl7.HL7MLLPNettyEncoderFactory;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.component.servlet.CamelHttpTransportServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
//import org.springframework.jms.connection.JmsTransactionManager;
//import javax.jms.ConnectionFactory;
import org.springframework.stereotype.Component;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

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

  @Bean
  ServletRegistrationBean camelServlet() {
    // use a @Bean to register the Camel servlet which we need to do
    // because we want to use the camel-servlet component for the Camel REST service
    ServletRegistrationBean mapping = new ServletRegistrationBean();
    mapping.setName("CamelServlet");
    mapping.setLoadOnStartup(1);
    mapping.setServlet(new CamelHttpTransportServlet());
    mapping.addUrlMappings("/camel/*");
    return mapping;
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
	 * Servlet Implementation
     */
    from("servlet://idassusercreds")
            .routeId("iDAASUserCreds")
            .convertBodyTo(String.class)
            // set Auditing Properties
            .setProperty("processingtype").constant("data")
            .setProperty("appname").constant("iDAAS-Connect-Demo")
            .setProperty("industrystd").constant("FHIR")
            .setProperty("messagetrigger").constant("Condition")
            .setProperty("component").simple("${routeId}")
            .setProperty("camelID").simple("${camelId}")
            .setProperty("exchangeID").simple("${exchangeId}")
            .setProperty("internalMsgID").simple("${id}")
            .setProperty("bodyData").simple("${body}")
            .setProperty("processname").constant("Input")
            .setProperty("auditdetails").constant("Consent message received")
            // iDAAS DataHub Processing
            .wireTap("direct:auditing")
            // Send To Topic
//            .convertBodyTo(String.class).to("kafka://localhost:9092?topic=fhirsvr_condition&brokers=localhost:9092")
//            .setHeader(Exchange.CONTENT_TYPE,constant("application/json"))
//            .to("jetty:http://localhost:8090/fhir-server/api/v4/Condition?bridgeEndpoint=true&exchangePattern=InOut")
//            //Process Response
//            .convertBodyTo(String.class)
//            //set Auditing Properties
//            .setProperty("processingtype").constant("data")
//            .setProperty("appname").constant("iDAAS-ConnectFinancial-IndustryStd")
//            .setProperty("industrystd").constant("FHIR")
//            .setProperty("messagetrigger").constant("consent")
//            .setProperty("component").simple("${routeId}")
//            .setProperty("processname").constant("Response")
//            .setProperty("camelID").simple("${camelId}")
//            .setProperty("exchangeID").simple("${exchangeId}")
//            .setProperty("internalMsgID").simple("${id}")
//            .setProperty("bodyData").simple("${body}")
//            .setProperty("auditdetails").constant("consent FHIR response message received")
            // iDAAS DataHub Processing
            .wireTap("direct:auditing")
    ;

  }
}