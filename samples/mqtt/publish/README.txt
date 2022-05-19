# mqtt-publish-sample

This is a sample of publishing message on mqtt bus using fty-messagebus2 interface

The message ""This is my test message" is publish on topic "/etn/samples/publish".
Before to send the message we bind a message lister function to the same topic to make sure we receive the message.

## What to learn here
* Create connection
* Build & send message
* Create message listener & subscrib to a topic