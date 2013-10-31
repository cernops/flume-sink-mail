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

public class MailSink extends AbstractSink implements Configurable {

  public static final String HOSTNAME   = "hostname";
  public static final String SENDER     = "sender";
  public static final String RECIPIENTS = "recipients";

  public static final int    DEFAULT_PORT = 25;

  private String   sender;
  private String[] recipients;
  private String   host;
  private int      port;

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
      
      // Get system properties
      Properties properties = System.getProperties();

      // Setup mail server
      properties.setProperty("mail.smtp.host", host);
      properties.put("mail.smtp.port", port);

      // Get the default Session object.
      Session session = Session.getDefaultInstance(properties);

      // Create a default MimeMessage object.
      MimeMessage message = new MimeMessage(session);

      // Set From: header field of the header.
      message.setFrom(new InternetAddress(sender));

      // Set To: header field of the header.
      for(String recipient : recipients) {
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient));
      }

      // Now set the subject and actual message
      Event event = ch.take();
      Map<String, String> headers = event.getHeaders();

      String host = new String(headers.get("host") == null ? "" : headers.get("host"));
      String prod = new String(headers.get("producer"));
      String body = new String(event.getBody());

      message.setSubject("Flume <" + host + "> " + prod);
      message.setText(body);

      // Send message
      Transport.send(message);

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
