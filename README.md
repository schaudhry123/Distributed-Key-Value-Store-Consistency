# Distributed Key-Value Store Consistency
#### A distributed key-value store with various options for its consistency model.

A replica server program that will store and get values, while carrying out consistency logic, along with clients that will issue put/get requests to a replica.

Consistency model options are eventual consistency and linearizability.

Partner MP with Samir Chaudhry and Kush Maheshwari. Pair programmed the entire assignment with Samir heading linearizability and Kush heading eventual consistency.

We implemented linearizability by setting one of our servers as a sequencer that all the other servers communicate with. Whenever a server that is not the sequencer receives a put or get request from a client, it sends that message to the sequencer. When the sequencer delivers a message from another server, it delivers it and multicasts it out all the other servers with the sequencer timestamp. When a server receives a multicast from the sequencer, it checks if the sequencer timestamp sent along with the message is equal to 1 + the current timestamp. If yes, the server delivers the message. Else, the server will put the message into the queue until its conditions are met. When the server that initially received the request from the client delivers the sequencer message, it will send its response back to the client. When a server other than that one receives delivers the sequencer message, it will simply deliver the message.