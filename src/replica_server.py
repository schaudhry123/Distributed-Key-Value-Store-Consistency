import sys
from threading import Thread, Lock
import socket
import pickle
import datetime
import time
import random
import signal
import Queue
import string

servers = []
server_id = 0
client_sockets = [] # client_sockets[i] = { 'id' = 1, 'socket'='', 'v_timestamp' = 1}
variables = {}
vector_timestamps = []
message_queue = []
mutex = Lock()

# Ran with commands "python basicMessages.py <#process_id>"
def main(argv):
	global server_id
	server_id = parse_file(argv[0])			# Get all servers info

	# Find the current process, setting all timestamps to 0
	server = None
	for process in servers:
		if (argv[0] in process):
			server = process

	# Initializing all variables to 0
	letters = string.ascii_lowercase
	for i in range(len(letters)):
		c = letters[i]
		variables[c] = 0

	vector_timestamps.append(0) # [0] - sequence timestamp
	vector_timestamps.append(0) # [1] - process timestamp

	# If process id could not be found
	if (server is None):
		print("Did not find process id " + argv[0 ] + " in config file.")
		print("Exiting.")
	else:
		# Create server and client threads
		try:
			server_thread = Thread(target=setup_server, args = (server[1], int(server[2]), server[0]))
			server_thread.daemon = True
			server_thread.start()

			# Find an elegant way to keep main running until done
			while True:
				time.sleep(100)

		except:
			print("Failed to start server.")


'''
Parses the config file for data about min/max delay and all servers info
'''
def parse_file(server_num):
	index = -1
	counter = 0
	with open('../configs/config.txt') as f:
		for line in f:
			process_info = line.split()
			if (counter == 0):
				global min_delay, max_delay
				min_delay = int(process_info[0])
				max_delay = int(process_info[1])
			else:
				servers.append(process_info)
			if (server_num == process_info[0]):
				index = counter-1
			counter += 1
	return servers


'''
Finds the process in the servers list with the given id
'''
def find_current_process(_id):
	current_process = None
	for process in servers:
		if (int(process[0]) == _id):
			current_process = process
	return current_process

'''
'''
def create_connection(process):
	# print("Trying to connect to " + process[0])
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	try:
		s.connect((process[1], int(process[2])))
		client_sockets.append( { 'id': process[0], 'socket': s })
		return True
	except:
		print("Unable to connect to process " + str(process[0]) + ". Process may not be started yet.")
		return False

'''
Multicasts a message out to all servers in the group given the message and current process info
'''
def multicast(message, current_process):

	mutex.acquire()
	vector_timestamps[0] += 1
	timestamp = vector_timestamps[0]
	mutex.release()

	for process in servers:
		if (process[0] != "0"):
			if (current_process[0] == "0"):
				unicast_send(message, current_process, process, timestamp, True)
			else:
				unicast_send(message, current_process, process, timestamp, False)
	if (current_process[0] == "0"):
		print("Delivered " + message + " from process " + current_process[0] + ", system time is " + str(datetime.datetime.now()).split(".")[0])

'''
Unicasts a message to the specified process given the message, current process info, and the specified process id
'''
def unicast_send(message, current_process, destinationInfo, timestamp, printSent):
	process_id = current_process[0]
	found = False
	# If the id is already in client_sockets
	for i in range(len(client_sockets)):
		# print("Have already connected to " + str(destinationInfo))
		if (destinationInfo[0] == client_sockets[i]['id']):
			found = True
			send_message(message, process_id, client_sockets[i], timestamp, printSent)

	# Else open up a new socket to the process
	if (not found):
		if (create_connection(destinationInfo)):
			send_message(message, process_id, client_sockets[len(client_sockets)-1], timestamp, printSent)

'''
Sends a message object to a process given the destination process info, the message, and the source process id
'''
def send_message(message, source, destination_process, timestamp, printSent):
	msg = {
			'message': message,
			'source': source,
			'destination': destination_process['id'],
			'timestamp': timestamp,
			'server': True
	}
	seralized_message = pickle.dumps(msg, -1)
	if (printSent):
		print("Sent " + message + " to process " + str(destination_process['id']) + ", system time is " + str(datetime.datetime.now()).split(".")[0])
	destination_process['socket'].sendall(seralized_message)

'''
Sets up a server for the current process that listens and accepts connections from other servers,
creating a new thread for each connection to a new process
'''
def setup_server(host, port, process_id):
	connections = []

	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((host, port))
	s.listen(len(servers))

	while True:
		conn, addr = s.accept()
		connections.append(conn)
		# print("Connected to: ", addr)
		conn_thread = Thread(target = readMessages, args = (conn,))
		conn_thread.start()

	for conn in connections:
		conn.close()

'''
Thread function that reads all messages sent from a process - responsible for only that process
'''
def readMessages(conn):
	while True:
		data = conn.recv(512)
		if (not data):
			break

		message_obj = pickle.loads(data)

		timestamp = message_obj['timestamp']
		message = str(message_obj['message'])
		source = int(message_obj['source'])
		destination = int(message_obj['destination'])
		server = 'server' in message_obj

		print("Receiving " + message)

		receive_message(message_obj)

		# receive_thread = Thread(target=receive_message, args = (message, source, destination, client_timestamp, True))
		# receive_thread.daemon = True
		# receive_thread.start()


def receive_message(message_obj):

	# mutex.acquire()
	# print("Received " + message + " from process " + str(source) + " with msg_timestamp = " + str(timestamp))
	# If the message should be delivered, deliver it
	if (delay_message(message_obj)):
		deliver_message(message_obj)
		# print("After deliver: Process timestamp = " + str(timestamp) + ", client timestamp = " + str(client_timestamp))

	# mutex.release()

def deliver_message(message_obj):
	message = message_obj['message']
	source = int(message_obj['source'])
	destination = int(message_obj['destination'])
	server = message_obj['server']

	mutex.acquire()
	# If not the sequencer, update the timestamp. Else multicast message out if not sent from itself
	if (destination != 0):
		vector_timestamps[0] += 1
	mutex.release()

	# If a server write -- write it to local variable
	if (server):
		if (message[0] == "p"):	# i.e. 'px2'
			write_variable(message)
	# Client request
	else:
		if (message[0] == "p"):
			multicast(message, servers)


	print("Delivered " + message + " from process " + str(source) + ", system time is " + str(datetime.datetime.now()).split(".")[0])

	if (destination == 0):
		current_process = find_current_process(source)
		multicast(message, current_process)

	# check_queue(destination)

'''
'''
def write_variable(message):
	var = message[1]
	value = int(message[2])
	variables[var] = value

'''
Delays the message if necessary by checking the vector timestamps
'''
def delay_message(message, source, destination, timestamp):

	# if (source != destination):
	time.sleep((random.uniform(min_delay, max_delay)/1000.0))	# Random network delay

	message = message_obj['message']
	source = int(message_obj['source'])
	destination = int(message_obj['destination'])
	timestamp = int(message_obj['timestamp'])
	server = message_obj['server']

	if (destination == 0):
		return True

	mutex.acquire()
	current_time = vector_timestamps[0]
	mutex.release()

	# Deliver the message
	if (timestamp == (current_time + 1) or timestamp == current_time):
		return True

	message_queue.append( {
							"message": message,
							"source": source,
							"destination": destination,
							"timestamp": timestamp,
							"server": server
						   })

	return False

'''
Checks the hold back queue for any messages that are waiting to be delivered
Looks at the timestamo of these messages
'''
def check_queue(process_id):
	mutex.acquire()
	timestamp = vector_timestamps[0]
	mutex.release()

	# print("Checking queue with timestamp " + str(timestamp))
	msg = None

	for message in message_queue:
		if (process_id == 0):
			message_queue.remove(message)
			msg = message
			break
		else:
			msg_timestamp = int(message["timestamp"])
			# print("Comparing msg w/ " + str(msg_timestamp) + " to current time of " + str(timestamp))
			if (msg_timestamp == (timestamp + 1) or (msg_timestamp == timestamp)):
				message_queue.remove(message)
				msg = message
				break
	if (msg):
		deliver_message(message["message"], message["source"], message["destination"], message["server"])


def handler(signum, frame):
	for i in range(len(client_sockets)):
		client_sockets[i]['socket'].close()
	sys.exit(0)

if __name__ == "__main__":
	signal.signal(signal.SIGINT, handler)
	if (len(sys.argv) != 2):
		print("python " + sys.argv[0] + " <#>")
	else:
		main(sys.argv[1:])