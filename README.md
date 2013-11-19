===============
flume-sink-mail
===============

==========
Parameters:

sender     => The sender of the mail
recipients => Comma separated list of recipients 
hostname   => The host to use to send the mail
port       => The port to use 
subject    => The subject of the mail. Can contain placeholders that will be 
              replaced with event values at run time (format: %{fieldname}).
              If the fieldname is "body" it will be replaced with the event 
              body, otherwise the field will be searched in the headers. If 
              it's not found it will be replaced by an empty string.
message    => The body of the mail. Can contain placeholders.

=======
Example:

es-mail-gw.sinks.k1.type       = org.cern.flume.sink.mail.MailSink
es-mail-gw.sinks.k1.channel    = c1
es-mail-gw.sinks.k1.sender     = mail@someserver
es-mail-gw.sinks.k1.recipients = mail1@example.com,mail2@example.com
es-mail-gw.sinks.k1.hostname   = localhost
es-mail-gw.sinks.k1.subject    = Flume <%{somefield}> %{somefield2}
es-mail-gw.sinks.k1.message    = %{somefield}

