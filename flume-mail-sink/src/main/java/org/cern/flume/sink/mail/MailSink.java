/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.cern.flume.sink.mail;

import java.lang.System;
import java.util.Properties;
import java.util.Map;

import javax.mail.*;
import javax.mail.internet.*;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import org.apache.commons.lang.StringUtils;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.util.List;
import java.util.ArrayList;

public class MailSink extends AbstractSink implements Configurable {

  public static final String HOSTNAME   = "hostname";
  public static final String SENDER     = "sender";
  public static final String RECIPIENTS = "recipients";
  public static final String SUBJECT    = "subject";  
  public static final String MESSAGE    = "message";

  public static final int    DEFAULT_PORT = 25;

  private String   sender;
  private String[] recipients;
  private String   host;
  private int      port;
  private String   subject;
  List<String>     subjectFields;
  private String   message;
  List<String>     messageFields;

  private static final Logger logger = LoggerFactory
      .getLogger(MailSink.class);

  @Override
  public void configure(Context context) {
    
    // Resolve address list 
    String[] hostName = null;
    if (StringUtils.isNotBlank(context.getString(HOSTNAME))) {
      hostName = context.getString(HOSTNAME).split(":");
    }
    host = hostName[0];
    port = hostName.length == 2 ? Integer.parseInt(hostName[1]) : DEFAULT_PORT;
    Preconditions.checkState( host != null, "Missing Param:" + HOSTNAME);   

    // Resolve recipient list
    if (StringUtils.isNotBlank(context.getString(RECIPIENTS))) {
      recipients = context.getString(RECIPIENTS).split(",");
    }
    Preconditions.checkState(recipients != null
      && recipients.length > 0, "Missing Param:" + RECIPIENTS);

    // Resolve sender
    if (StringUtils.isNotBlank(context.getString(SENDER))) {
      sender = context.getString(SENDER);
    }
    Preconditions.checkState(sender != null, "Missing Param:" + SENDER);

    // Resolve message structure
    if (StringUtils.isNotBlank(context.getString(SUBJECT))) {
      subject = context.getString(SUBJECT);
    }
    Preconditions.checkState(subject != null, "Missing Param:" + SUBJECT);
    
    // Generate the messageFields collection
    subjectFields = new ArrayList<String>();
    Matcher m = Pattern.compile("%\\{(.*?)\\}").matcher(subject);
    while (m.find()) {
      subjectFields.add( m.group(1) );
      logger.info("Parsing subject field: {}", m.group(1) );
    }

    // Resolve message structure
    if (StringUtils.isNotBlank(context.getString(MESSAGE))) {
      message = context.getString(MESSAGE);
    }
    Preconditions.checkState(message != null, "Missing Param:" + MESSAGE);
    
    // Generate the messageFields collection
    messageFields = new ArrayList<String>();
    m = Pattern.compile("%\\{(.*?)\\}").matcher(message);
    while (m.find()) {
      messageFields.add( m.group(1) );
      logger.info("Parsing message field: {}", m.group(1) );
    }

  }

  @Override
  public void start() {
    // Initialize the connection to the external repository (e.g. HDFS) that
    // this Sink will forward Events to ..
  }

  @Override
  public void stop () {
    // Disconnect from the external respository and do any
    // additional cleanup (e.g. releasing resources or nulling-out
    // field values) ..
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = null;

    // Start transaction
    Channel ch = getChannel();
    Transaction txn = ch.getTransaction();
    txn.begin();

    try {
      
      Event event = ch.take();

      if ( event != null ) {

        // Get system properties
        Properties properties = System.getProperties();

        // Setup mail server
        properties.setProperty("mail.smtp.host", host);
        properties.put("mail.smtp.port", port);

        // Get the default Session object.
        Session session = Session.getDefaultInstance(properties);

        // Create a default MimeMessage object.
        MimeMessage mimeMessage = new MimeMessage(session);

        // Set From: header field of the header.
        mimeMessage.setFrom(new InternetAddress(sender));

        // Set To: header field of the header.
        for(String recipient : recipients) {
          mimeMessage.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient));
        }

        // Now set the subject and actual message
        Map<String, String> headers = event.getHeaders();

        String value;

        String mailSubject = subject;
        for ( String field : subjectFields ) {

          try {
            if ( field.equals("body") ) {
              value = new String( event.getBody() );
            } else {
              value = new String( headers.get(field) );
            }
          } catch (NullPointerException t) {
            value = "";
          }

          mailSubject = mailSubject.replace("%{" + field + "}", value);
        }

        String mailMessage = message;
        for ( String field : messageFields ) {

          try {
            if ( field.equals("body") ) {
              value = new String( event.getBody() );
            } else {
              value = new String( headers.get(field) );
            }
          } catch (NullPointerException t) {
            value = "";
          }

          mailMessage = mailMessage.replace("%{" + field + "}", value);
        }

        mimeMessage.setSubject(mailSubject);
        mimeMessage.setText(mailMessage);

        // Send message
        Transport.send(mimeMessage);

      }

      txn.commit();
      status = Status.READY;

    } catch (Throwable t) {

      txn.rollback();

      logger.error("Unable to send e-mail.", t);

      status = Status.BACKOFF;

      if (t instanceof Error) {
        throw (Error)t;
      }

    } finally {

      txn.close();

    }

    return status;
  }
}
