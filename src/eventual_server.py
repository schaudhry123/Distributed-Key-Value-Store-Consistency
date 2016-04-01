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
sockets = [] # sockets[i] = { 'id' = 1, 'socket'='', 'open' = True}
client_sockets = [] # Array of sockets of all clients connected
variables = {}
vector_timestamp = 0
message_queue = []
mutex = Lock()
writes = 0
reads = 0

message_ids_lock = Lock()
message_ids = 0

output = ""
output_file = None
session_num = 0
output_mutex = Lock()

# Ran with commands "python basicMessages.py <#process_id>"
def main(argv):
	global server_id, writes, reads, session_num
	server_id = parse_file(int(argv[0]))			# Get all servers info
	session_num = (server_id+1) * 100

	writes = int(argv[1])
	reads = int(argv[2])

	# If process_id could not be found
	if (server_id == -1):
		print("Did not find process id " + argv[1] + " in config file.")
		print("Exiting.")
	else:
		# Initializing all variables to -1 and timestamps of variables to 0(an invalid #)
		letters = string.ascii_lowercase
		for i in range(len(letters)):
			c = letters[i]
			variables[c] = [0,-1]


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

def find_process(_id):
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
'''
def create_connection(process):
	# print("Trying to connect to " + process[0])
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	try:
		s.connect((process[1], int(process[2])))
		sockets.append( { 'id': process[0], 'socket': s, 'open': True })
		return True
	except:
		print("Unable to connect to process " + str(process[0]) + ". Process may not be started yet.")
		return False

'''
Multicasts a message out to all servers in the group given the message and current process info
'''
def multicast(message_obj, current_process, client_socket_index):
	for process in servers:
		if(process[0] != current_process[0]):
			unicast_send(message_obj, current_process, process, client_socket_index)

# message_obj takes in message, timestamp, message_id, acknowledgement

'''
Unicasts a message to the specified process given the message, current process info, and the specified process id
'''
def unicast_send(message_obj, current_process, destinationInfo, client_socket_index):
	source = current_process[0]		# Get the source
	found = False
	# If the id is already in sockets
	for i in range(len(sockets)):
		if (destinationInfo[0] == sockets[i]['id']):
			found = True
			send_message(message_obj, source, sockets[i], client_socket_index)

	# Else open up a new socket to the process
	if (not found):
		if (create_connection(destinationInfo)):
			send_message(message_obj, source, sockets[len(sockets)-1], client_socket_index)

'''
Sends a message object to a process given the destination process info, the message, and the source process id
'''
def send_message(message_obj, source, destination_process, client_socket_index):
	msg = {
			'message': message_obj['message'],
			'source': int(source),
			'destination': destination_process['id'],
			'timestamp': message_obj['timestamp'],
			'process_type': 'server',
			'client_socket_index': client_socket_index,
			'message_id': message_obj['message_id'],
			'acknowledgement': message_obj['acknowledgement'],
	}

	if (destination_process['open']):
		try:
			serialized_message = pickle.dumps(msg, -1)
			destination_process['socket'].sendall(serialized_message)
		except Exception,e:
			print("Error: Cannot communicate with server " + destination_process['id'] + ". Will assume crashed.")
			for i in range(len(sockets)):
				if (destination_process['id'] == sockets[i]['id']):
					sockets[i]['open'] = False
					break
			print(str(e))


'''
Thread function that reads all messages sent from a process - responsible for only that process
'''
def readMessages(conn):
	while True:
		data = conn.recv(1024)
		if (not data):
			break

		message_obj = pickle.loads(data)

		process_type = message_obj['process_type']

		client_socket_index = -1

		if (process_type == "server"):
			# source = int(message_obj['source'])
			client_socket_index = message_obj['client_socket_index']
		# Else, sent from a client
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
		receive_thread.daemon = True
		receive_thread.start()


def receive_message(message_obj, client_socket_index):
	# If the message should be delivered, deliver it
	if (delay_message(message_obj, client_socket_index)):
		mutex.acquire()
		deliver_message(message_obj, client_socket_index)
		mutex.release()


"""
"""
def deliver_message(message_obj, client_socket_index):
	message = message_obj['message']
	destination = int(message_obj['destination'])
	process_type = message_obj['process_type']
	# Get current server info
	current_process = find_process(destination)

	# If delivering message from client
	if (process_type == "client"):
		# Else if a dump message
		if (message == "d"):
			dump_variables(current_process, client_socket_index)

	# Else delivering message from server (sequencer)
	else:

		source = int(message_obj['source'])
		message_id = message_obj['message_id']
		acknowledgement = message_obj['acknowledgement']

		# If a multicast from a process
		if (not acknowledgement):
			# Update your variable depending on the message's timestamps
			var = update_variable(message_obj)

			message = message[0] + message[1] + str(var[0]) # format of px3 or gx1
			timestamp = var[1]
			destinationInfo = find_process(source)

			message_obj2 = {
							'message': message,
							'timestamp': timestamp,
							'message_id': message_id,
							'acknowledgement': True
					  	}
			unicast_send(message_obj2, current_process, destinationInfo, client_socket_index)
		# Else an acknowledgement
		else:
			var = update_variable(message_obj)

			# Find message in message_queue
			for msg in message_queue:
				msg_obj = msg[0]

				# Find the message in the message_queue based off message_id
				if(msg_obj['message_id'] == message_id):

					var = update_variable(msg_obj)
					msg[1] += 1
					# print("Acknowledgements = " + str(msg[1]))
					if(msg_obj['message'][0] == 'p'):
						# print("Comparing with " + str(writes))
						if(msg[1] >= writes):
							print("Received enough acknowledges to return write")
							message_queue.remove(msg)
							respond_to_client(msg_obj, client_socket_index)
					elif(msg_obj['message'][0] == 'g'):
						if(msg[1] >= reads):
							print("Recieved enough acknowledges to return read")
							message_queue.remove(msg)
							respond_to_client(msg_obj, client_socket_index)
					break
		# print("Delivered " + message + " from process " + str(source) + ", system time is " + str(datetime.datetime.now()).split(".")[0])



"""

"""

def respond_to_client(message_obj, client_socket_index):
	message = message_obj['message']
	if (not 'source' in message_obj):
		message_obj['source'] = message_obj['destination'] - 1

	# value = update_variable(message_obj)
	var = variables[message[1]]
	value = var[0]

	# If variable has been found
	if (value != -1):
		write_to_file(message, "resp", client_socket_index, value)
		message_obj = { 'message': 'A - ' + str(var) }
	# Else respond with nothing
	else:
		write_to_file(message, "resp", client_socket_index, "")
		message_obj = { 'message': 'A - ' + message[1] + " not found" }
	serialized_message = pickle.dumps(message_obj, -1)
	client_sockets[client_socket_index].sendall(serialized_message)


def update_variable(message_obj):
	message = message_obj['message']
	timestamp = message_obj['timestamp']
	source = int(message_obj['source'])
	destination = int(message_obj['destination'])

	var = message[1]
	# If a put, update the variable (format = px3)
	if (message[0] == "p"):
		print(message + ": comparing current time " + str(variables[var][1]) + " to source " + str(source) + " with time " + str(timestamp))
		if(variables[var][1] < timestamp):
			print("Using message with newer timestamp: " + message)
			variables[var][0] = int(message[2])
			variables[var][1] = timestamp
		elif (variables[var][1] == timestamp):
			if (source > destination):
				print("Using message with same timestamp and higher process #: " + message)
				variables[var][0] = int(message[2])
				variables[var][1] = timestamp
			else:
				print("Using current message with same timestamp b/c of higher process #: " + message)
		else:
			print("Using current message with newer timestamp: " + message)
		return variables[var]
	# Else, message is a get
	else:
		# If the received get does not have that variable (i.e = -1)
		if (message[2] != "-"):
			if (timestamp > variables[var][1]):
				print("Using message with newer timestamp: " + message)
				variables[var][0] = int(message[2])
				variables[var][1] = timestamp
			# elif (variables[var][1] == timestamp):
				# if (source > destination):
					# print("Using message with same timestamp and higher process #: " + message)
					# variables[var][0] = int(message[2])
					# variables[var][1] = timestamp
				# else:
					# print("Using current message with same timestamp b/c of higher process #: " + message)
			else:
				print("Using current message with newer/equa timestamp: " + message)
		return variables[var]


"""

"""
def dump_variables(current_process, client_socket_index):
	message = ''
	for var in variables:
		if (variables[var][1] != -1):
			message += var + ": " + str(variables[var]) + "\n"

	print(message)

	message = 'A'

	message_obj = { 'message': message }
	serialized_message = pickle.dumps(message_obj, -1)
	client_sockets[client_socket_index].sendall(serialized_message)

'''
Delays the message if necessary by checking the vector timestamps
'''
def delay_message(message_obj, client_socket_index):
	process_type = message_obj['process_type']

	if (process_type == "server"):
		time.sleep((random.uniform(min_delay, max_delay)/1000.0))	# Random network delay
		return True
	elif (process_type == "client"):
		message = message_obj['message']

		write_to_file(message, "req", client_socket_index, -1)

		if (message[0] == "p" or message[0] == "g"):
			message = message_obj['message']
			# source = int(message_obj['source'])
			destination = int(message_obj['destination'])

			# Get timestamp
			var = variables[message[1]]

			timestamp = var[1]
			if (message[0] == "p"):
				timestamp += 1

			if (message[0] == "g"):
				message += str(var[0])

			message_ids_lock.acquire()
			global message_ids
			message_ids += 1
			message_id = message_ids
			message_ids_lock.release()

			message_queue.append( [{
							'message': message,
							'source': -1,
							'destination': destination,
							'timestamp': timestamp,
							'client_socket_index': client_socket_index,
							'message_id': message_id,
					  	}, 0])

			current_process = find_process(destination)

			# Message we multicast out to other servers, waiting for acknowledgemtn
			message_obj2 = {
							'message': message,
							'timestamp': timestamp,
							'message_id': message_id,
							'acknowledgement': False
							}

			multicast(message_obj2, current_process, client_socket_index)
		# Else a dump
		else:
			return True
	return False

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
	else:
		print("Invalid message_type")
	output_mutex.release()

'''
SIGINT handler to close all sockets on signal
'''
def handler(signum, frame):
	for i in range(len(sockets)):
		sockets[i]['socket'].close()
	for socket in client_sockets:
		socket.close()

	output_file.close()
	sys.exit(0)

if __name__ == "__main__":
	signal.signal(signal.SIGINT, handler)
	if (len(sys.argv) != 4):
		print("python " + sys.argv[0] + " <process #>")
	else:
		main(sys.argv[1:])