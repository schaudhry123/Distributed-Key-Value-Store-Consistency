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
server_id = -1
sockets = [] # sockets[i] = { 'id' = 1, 'socket'='', 'v_timestamp' = 1}
client_sockets = [] # Array of sockets of all clients connected
variables = {} # Dictionary mapping variables to values
vector_timestamp = 0
message_queue = []
mutex = Lock()

output = ""
output_file = None
session_num = 0
output_mutex = Lock()

# Ran with commands "python replica_server.py <#process_id>"
def main(argv):
	global server_id, session_num
	server_id = parse_file(int(argv[0]))			# Get all servers info
	session_num = (server_id+1) * 100

	# If process_id could not be found
	if (server_id == -1):
		print("Did not find process id " + argv[1] + " in config file.")
		print("Exiting.")
	else:
		# Initializing all variables to -1 (an invalid #)
		letters = string.ascii_lowercase
		for i in range(len(letters)):
			c = letters[i]
			variables[c] = -1

		# Get server info
		server = servers[server_id]

		# Open up the output file for writing
		global output_file
		output_file = open("../logs/output_log" + str(server[0]) + ".txt", "w")

		# Create server thread

		try:
			server_thread = Thread(target=setup_server, args = (server[1], int(server[2]), server[0]))
			server_thread.daemon = True
			server_thread.start()

			# Find an elegant way to keep main running until done
			while True:
				time.sleep(100)
		except Exception,e:
			print("Failed to start server.")
			print (str(e))


'''
Parses the config file for data about min/max delay and all servers info
'''
def parse_file(server_num):
	counter = 0
	num_lines = 0
	index = -1

	with open('../configs/config.txt') as f:
		for line in f:
			num_lines += 1

	# Wrap around from highest server to lowest server
	if (server_num != num_lines-1):
		server_num = server_num % (num_lines - 1)

	with open('../configs/config.txt') as f:
		for line in f:
			process_info = line.split()
			if (counter == 0):
				global min_delay, max_delay
				min_delay = int(process_info[0])
				max_delay = int(process_info[1])
			else:
				servers.append(process_info)
			if (server_num == int(process_info[0])):
				index = counter - 1
			counter += 1
	return index

"""
Given an id, finds the process/server with that id and returns its info
"""
def find_current_process(_id):
	current_process = None
	for process in servers:
		if (int(process[0]) == _id):
			current_process = process
			break
	return current_process

'''
Sets up the client for the current process, reading input from the command line and unicasting/multicasting it out
'''
def setup_client(current_process):
	global vector_timestamp
	while True:
		user_input = raw_input('')

		if (user_input == "exit"):
			break;
		if (user_input):
			message_thread = Thread(target=message_send, args = (user_input, current_process))
			message_thread.daemon = True
			message_thread.start()

	print("Exiting client")
	for i in range(len(sockets)):
		sockets[i]['socket'].close()

'''
Creates a new connection with a process given its info (id, host, port)
Returns true if a socket connection could be made
Appends the new socket and id information to the sockets list
'''
def create_connection(process):
	# print("Trying to connect to " + process[0])
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	try:
		s.connect((process[1], int(process[2])))
		sockets.append( { 'id': process[0], 'socket': s })
		return True
	except:
		print("Unable to connect to process " + str(process[0]) + ". Process may not be started yet.")
		return False

'''
Multicasts a message out to all processes (except the sequencer)
Will only be called by the sequencer
"Mocks" sending a message to itself, simulates a delivery with no wait
'''
def multicast(message, current_process, client_socket_index):
	for process in servers:
		if (process[0] != "1"):
			unicast_send(message, current_process, process, vector_timestamp, client_socket_index)
	if (current_process[0] == "1"):
		print("Delivered " + message + " from process " + current_process[0] + ", system time is " + str(datetime.datetime.now()).split(".")[0])

'''
Finds the destination's socket in list of sockets
If the socket is not there, creates a new connection
Calls send_message to actually send message over the socket
'''
def unicast_send(message, current_process, destinationInfo, timestamp, client_socket_index):
	process_id = current_process[0]
	found = False
	# If the id is already in sockets
	for i in range(len(sockets)):
		if (destinationInfo[0] == sockets[i]['id']):
			found = True
			send_message(message, process_id, sockets[i], timestamp, client_socket_index)

	# Else open up a new socket to the process
	if (not found):
		if (create_connection(destinationInfo)):
			send_message(message, process_id, sockets[len(sockets)-1], timestamp, client_socket_index)

'''
Actually sends the message and other information over a socket connecting to the destination
'''
def send_message(message, source, destination_process, timestamp, client_socket_index):
	msg = {
			'message': message,
			'source': source,
			'destination': destination_process['id'],
			'timestamp': timestamp,
			'process_type': 'server',
			'client_socket_index': client_socket_index,
	}
	serialized_message = pickle.dumps(msg, -1)
	destination_process['socket'].sendall(serialized_message)

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
		conn_thread = Thread(target = readMessages, args = (conn,))
		conn_thread.start()

	for conn in connections:
		conn.close()

'''
Thread function that reads all messages sent from a process - responsible for only that process
Creates a new thread for every message received
'''
def readMessages(conn):
	while True:
		data = conn.recv(1024)
		if (not data):
			break

		message_obj = pickle.loads(data)

		process_type = message_obj['process_type']
		client_socket_index = -1

		# If message is from a server, message includes a client_socket_index
		if (process_type == "server"):
			client_socket_index = message_obj['client_socket_index']
		# Else message is sent from a client, so find this client_socket
		else:
			# Check if the socket is already in client_sockets
			for index in range(len(client_sockets)):
				if (client_sockets[index] == conn):
					client_socket_index = index
					break
			# If not, create a new object in client_sockets
			if (client_socket_index == -1):
				client_socket_index = len(client_sockets)
				client_sockets.append(conn)


		receive_thread = Thread(target=receive_message, args = (message_obj, client_socket_index))
			# message, source, destination, timestamp, process_type, client_socket_index))
		receive_thread.daemon = True
		receive_thread.start()

"""
Delays the message and delivers it if the conditions have been met
"""
def receive_message(message_obj, client_socket_index):
	# message = message_obj['message']
	# timestamp = message_obj['timestamp']
	# print("Receiving " + message + " w/ timestamp " + str(timestamp) + " with own time " + str(vector_timestamp))

	# If the message should be delivered, deliver it
	if (delay_message(message_obj, client_socket_index)):
		mutex.acquire()
		deliver_message(message_obj, client_socket_index)
		mutex.release()

"""
Handles the delivering of a message, broadcasting it if necessary
If a message is from a client, sends the message to the sequencer
"""
def deliver_message(message_obj, client_socket_index):
	global vector_timestamp

	message = message_obj['message']
	destination = int(message_obj['destination'])
	timestamp = int(message_obj['timestamp'])
	process_type = message_obj['process_type']

	# If delivering message from client
	if (process_type == "client"):
		# Get current server info
		current_process = find_current_process(destination)

		# If a put or a get request that requires communication with other replica servers
		if (message[0] == "p" or message[0] == "g"):

			# If current process is the sequencer, deliver and multicast message to all other servers
			if (destination == 1):
				time.sleep((random.uniform(min_delay, max_delay)/1000.0))	# Random network delay

				vector_timestamp += 1

				multicast(message, current_process, client_socket_index)

				value = update_variable(message)

				if (message[0] == "g"):
					message += str(value)

				# After multicasting, respond to the client
				respond_to_client(message, client_socket_index, value)

			# Else, send it to the sequencer for total order broadcasting
			else:
				sequencer = find_current_process(1)
				unicast_send(message, current_process, servers[0], vector_timestamp, client_socket_index)
		# Else if client request is a dump, dump variables
		elif (message == "d"):
			dump_variables(current_process, client_socket_index)

	# Else delivering message from a server
	else:
		source = int(message_obj['source'])

		# Update the timestamp
		vector_timestamp += 1

		value = -1

		print("Delivered " + message + " from process " + str(source) + ", system time is " + str(datetime.datetime.now()).split(".")[0])

		# Update the variable if message is a write
		value = update_variable(message)

		# If the sequencer, multicast message out to others
		if (destination == 1):
			current_process = find_current_process(source)
			print(current_process[0])
			multicast(message, current_process, client_socket_index)

		# If you delivered the request to yourself, send acknowledgment/response to client
		if (source == destination):
			respond_to_client(message, client_socket_index, value)

		# Check the message_queue for any pending messages to see if timestamp conditions have been filled
		check_queue(destination)

"""
Responds back to the client, writing the response to the output log file
"""
def respond_to_client(message, client_socket_index, value):
	write_to_file(message, "resp", client_socket_index, value)

	# Send back acknowledgment with returned value
	message_obj = { 'message': 'A', 'value': str(value) }
	serialized_message = pickle.dumps(message_obj, -1)
	client_sockets[client_socket_index].sendall(serialized_message)

'''
Immediately returns True for delivering if message is from a client
If from a server, adds random network delay, delivering if the sequencer or timestamps match up
If timestamps do not match, adds message to the message_queue
'''
def delay_message(message_obj, client_socket_index):
	process_type = message_obj['process_type']
	message = message_obj['message']

	# If the message is from the client, log the request and deliver the message
	if (process_type == "client"):
		write_to_file(message, "req", client_socket_index, -1)
		return True

	# Else, message is from server
	time.sleep((random.uniform(min_delay, max_delay)/1000.0))	# Random network delay

	mutex.acquire()
	v_timestamp = vector_timestamp
	mutex.release()

	source = int(message_obj['source'])
	destination = int(message_obj['destination'])
	timestamp = int(message_obj['timestamp'])

	# If sequencer is receiving the message, immediately deliver
	if (destination == 1):
		return True

	# If timestamps match up, deliver the message
	if (timestamp == (v_timestamp + 1) or timestamp == v_timestamp):
		return True

	# Put message into the queue until timestamp has been updated enough
	# print("Putting message into queue")
	message_queue.append({
							'message': message,
							'source': source,
							'destination': destination,
							'timestamp': timestamp,
							'process_type': 'server',
							'client_socket_index': client_socket_index,
					  	})

	return False

def check_queue(process_id):
	msg = None

	for message in message_queue:
		if (process_id == 1):
			message_queue.remove(message)
			msg = message
			break
		else:
			msg_timestamp = int(message['timestamp'])
			# print("Comparing msg w/ " + str(msg_timestamp) + " to current time of " + str(timestamp))
			if (msg_timestamp == (vector_timestamp + 1) or (msg_timestamp == vector_timestamp)):
				message_queue.remove(message)
				msg = message
				break
	if (msg):
		deliver_message(msg, msg['client_socket_index'])

"""
Updates the variable if a put and returns the value, else returns the value on a get
"""
def update_variable(message):
	var = message[1]
	# If a put, update the variable (format = px3)
	if (message[0] == "p"):
		variables[var] = int(message[2])
		return variables[var]
	# Else, get the variable value (format = gx)
	else:
		return variables[var]


"""
Dumps all variables that have been initialized
"""
def dump_variables(current_process, client_socket_index):
	message = ''
	for var in variables:
		if (variables[var] != -1):
			message += var + ": " + str(variables[var]) + "\n"

	print(message)

	message = 'A'

	message_obj = { 'message': message }
	serialized_message = pickle.dumps(message_obj, -1)
	client_sockets[client_socket_index].sendall(serialized_message)

def write_to_file(message, message_type, client_socket_index, value):
	request_type = ""
	if (message[0] == "p"):
		request_type = "put"
	elif (message[0] == "g"):
		request_type = "get"

	output_mutex.acquire()
	if (request_type):
		request_var = message[1]
		cur_time = str(int(time.time() * 1000))

		output = str(session_num) + "," + str(client_socket_index) + "," + request_type + "," + request_var + "," + cur_time + "," + message_type + ","

		# If a put request or a response, add the value
		if (request_type == "put" or message_type == "resp"):
			if (value == -1):
				value = int(message[2])
			output += str(value)
		output += "\n"

		output_file.write(output)
		output_file.flush()
	output_mutex.release()

def handler(signum, frame):
	for i in range(len(sockets)):
		sockets[i]['socket'].close()

	output_file.close()
	sys.exit(0)

if __name__ == "__main__":
	signal.signal(signal.SIGINT, handler)
	if (len(sys.argv) != 2):
		print("python " + sys.argv[0] + " <process #>")
	else:
		main(sys.argv[1:])