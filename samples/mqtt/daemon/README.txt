# mqtt-daemon-basic-sample

This is a sample of daemon / client on mqtt bus using fty-messagebus2 interfaces.

## daemon
The deamon `mqtt-daemon-basic-sample` is listening to the queue "/etn/samples/daemon-basic/mailbox" for new messages.
The dameon accept the message with subject "TO_UPPER", the daemon will convert all the characters from the payload to upper characters.

The daemon can be interupted by CTRL-C.

## client
The client 'mqtt-daemon-basic-sample-cli` is taking as argument a string to convert to upper characters.
The client will request the daemon do to the change to upper and read the reply form it.

## What to learn here
* Create connection
* Create process function & subscrib to a queue
* Build & send request
* Build & send reply
* Check and extract data from reply