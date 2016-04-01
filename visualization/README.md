To run the visualization program:
1.  cd into /visualization folder
2.  Run "python server/server.py". This program listens to localhost port 1234
    for log entries. The format of the log entries is outlined below. This program
    can receive log entries out of order. To kill the program Ctrl+Shift+\.
3.  Run "html/line_series_test.html" in a web browser. To output the visualization
    of the log, you need to provide it with the SessionID. That's the same
    SessionID attached to the log entries.

To start/restart a log session, you need to send server.py a string of the format:
START,SessionID
This will let server.py know where the first entry of the current log session is.

Individual log entries should be strings of comma separated values. The values
must include the following in this exact order:

1. SessionID of the current session. (For the mp, can just use any ID)
2. Integer ID of the client.
3. Request type, which is either "get" or "put".
4. Variable name.
5. Logical timestamp.
6. Either "req" or "resp", which denote request/response.
7. Value (could be empty)

Example of how send log entries to server.py 
1. using terminal:
echo -n "START,SessionNumber1" | nc -4u -w1 127.0.0.1 1234
echo -n "SessionNumber1,2,get,y,132,req," | nc -4u -w1 127.0.0.1 1234
echo -n "SessionNumber1,2,get,y,159,resp,A" | nc -4u -w1 127.0.0.1 1234
echo -n "SessionNumber1,0,put,x,220,req,B" | nc -4u -w1 127.0.0.1 1234
echo -n "SessionNumber1,0,put,x,230,resp,B" | nc -4u -w1 127.0.0.1 1234

2. as a python script:
import socket
import time

IP = "127.0.0.1"
PORT = 1234

if __name__ == '__main__':
  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  sock.sendto("START,SessionNumber1", (IP, PORT))
  sock.sendto("SessionNumber1,2,get,y,132,req,", (IP, PORT))
  sock.sendto("SessionNumber1,2,get,y,159,resp,A", (IP, PORT))
  sock.sendto("SessionNumber1,0,put,x,220,req,B", (IP, PORT))
  sock.sendto("SessionNumber1,0,put,x,230,resp,B", (IP, PORT))

How to interpret the visualization.
The horizontal axis shows the relative order of the events, while the vertical axis 
shows the client number. All PUT requests are colored black. A GET request is GREEN
if it returned the last value of the variable, YELLOW if it returned the second to 
last value, RED if it returned an even older value. Lines connecting a YELLOW GET 
request to a PUT request, shows where the stale value came from.
