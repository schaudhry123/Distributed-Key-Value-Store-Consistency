import sys
from threading import Thread, Lock
import socket
import pickle
import datetime
import time
import random
import signal
import Queue

processes = []
client_sockets = [] # client_sockets[i] = { 'id' = 1, 'socket'='', 'v_timestamp' = 1}
vector_timestamp = 0
message_queue = []
mutex = Lock()

# Ran with commands "python basicMessages.py <#process_id>"
def main(argv):
	processes = parse_file()			# Get all processes info

	# Find the current process, setting all timestamps to 0
	current_process = None
	for process in processes:
		if (argv[0] in process):
			current_process = process

	# If process id could not be found
	if (current_process is None):
		print("Did not find process id " + argv[1] + " in config file.")
		print("Exiting.")
	else:
		# Create server and client threads
		try:
			server_thread = Thread(target=setup_server, args = (current_process[1], int(current_process[2]), current_process[0]))
			server_thread.daemon = True
			server_thread.start()

			client_thread = Thread(target=setup_client, args = (current_process,))
			client_thread.daemon = True
			client_thread.start()
		except:
			print("Failed to start server.")

	# Find an elegant way to keep main running until done
	while True:
		time.sleep(100)

'''
Parses the config file for data about min/max delay and all processes info
'''
def parse_file():
	index = 0
	with open('../configs/config.txt') as f:
		for line in f:
			process_info = line.split()
			if (index == 0):
				global min_delay, max_delay
				min_delay = int(process_info[0])
				max_delay = int(process_info[1])
			else:
				processes.append(process_info)
			index += 1
	return processes

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
	for i in range(len(client_sockets)):
		client_sockets[i]['socket'].close()

def message_send(user_input, current_process):
	global vector_timestamp

	input_split = user_input.split()
	message = ''

	process_id = int(current_process[0])

	# Multicasting
	if (input_split[0] == "msend"):

		message = user_input[6:]
		if (current_process[0] != "1"):
			unicast_send(message, current_process, processes[0], 0)
			for process in processes:
				print("Sent " + message + " to process " + str(process[0]) + ", system time is " + str(datetime.datetime.now()).split(".")[0])
		else:
			for process in processes:
				print("Sent " + message + " to process " + str(process[0]) + ", system time is " + str(datetime.datetime.now()).split(".")[0])
			mutex.acquire()
			vector_timestamp += 1
			multicast(message, current_process)
			mutex.release()

	# Unicasting
	elif (input_split[0] == "send" and input_split[1].isdigit()):
		destination_id = int(input_split[1])
		message = user_input[7:]
		unicast_send(message, current_process, destination_id)
	else:
		print("msend <message>")
		print("send <#> <message>")


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
Multicasts a message out to all processes in the group given the message and current process info
'''
def multicast(message, current_process):

	for process in processes:
		if (process[0] != "1"):
			unicast_send(message, current_process, process, vector_timestamp)
	if (current_process[0] == "1"):
		print("Delivered " + message + " from process " + current_process[0] + ", system time is " + str(datetime.datetime.now()).split(".")[0])

'''
Unicasts a message to the specified process given the message, current process info, and the specified process id
'''
def unicast_send(message, current_process, destinationInfo, timestamp):
	process_id = current_process[0]
	found = False
	# If the id is already in client_sockets
	for i in range(len(client_sockets)):
		if (destinationInfo[0] == client_sockets[i]['id']):
			found = True
			send_message(message, process_id, client_sockets[i], timestamp)

	# Else open up a new socket to the process
	if (not found):
		if (create_connection(destinationInfo)):
			send_message(message, process_id, client_sockets[len(client_sockets)-1], timestamp)

'''
Sends a message object to a process given the destination process info, the message, and the source process id
'''
def send_message(message, source, destination_process, timestamp):
	msg = {
			'message': message,
			'source': source,
			'destination': destination_process['id'],
			'timestamp': timestamp
	}
	seralized_message = pickle.dumps(msg, -1)
	destination_process['socket'].sendall(seralized_message)

'''
Sets up a server for the current process that listens and accepts connections from other processes,
creating a new thread for each connection to a new process
'''
def setup_server(host, port, process_id):
	connections = []

	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((host, port))
	s.listen(len(processes))

	while True:
		conn, addr = s.accept()
		connections.append(conn)
		conn_thread = Thread(target = readMessages, args = (conn,))
		conn_thread.start()

	for conn in connections:
		conn.close()

'''
Thread function that reads all messages sent from a process - responsible for only that process
'''
def readMessages(conn):
	while True:
		data = conn.recv(1024)
		if (not data):
			break

		message_obj = pickle.loads(data)

		timestamp = message_obj['timestamp']
		message = str(message_obj['message'])
		source = int(message_obj['source'])
		destination = int(message_obj['destination'])

		receive_thread = Thread(target=receive_message, args = (message, source, destination, timestamp))
		receive_thread.daemon = True
		receive_thread.start()


def receive_message(message, source, destination, timestamp):
	# print("Receiving " + message + " w/ timestamp " + str(timestamp) + " with own time " + str(vector_timestamp))

	# If the message should be delivered, deliver it
	if (delay_message(message, source, destination, timestamp)):
		mutex.acquire()
		deliver_message(message, source, destination)
		mutex.release()


def deliver_message(message, source, destination):
	global vector_timestamp
	# If not the sequencer, update the timestamp. Else multicast message out if not sent from itself
	vector_timestamp += 1

	print("Delivered " + message + " from process " + str(source) + ", system time is " + str(datetime.datetime.now()).split(".")[0])

	if (destination == 1):
		current_process = None
		for process in processes:
			if (int(process[0]) == source):
				current_process = process
		multicast(message, current_process)

	check_queue(destination)

'''
Delays the message if necessary by checking the vector timestamps
'''
def delay_message(message, source, destination, timestamp):
	time.sleep((random.uniform(min_delay, max_delay)/1000.0))	# Random network delay

	mutex.acquire()
	v_timestamp = vector_timestamp
	mutex.release()

	if (destination == 1):
		return True

	# Deliver the message
	if (timestamp == (v_timestamp + 1) or timestamp == v_timestamp):
		return True

	# print("Putting '" + message + "' into queue with timestamp = " + str(timestamp))
	message_queue.append((message, source, destination, timestamp))

	return False

def check_queue(process_id):
	msg = None

	for message in message_queue:
		if (process_id == 1):
			message_queue.remove(message)
			msg = message
			break
		else:
			msg_timestamp = int(message[3])
			# print("Comparing msg w/ " + str(msg_timestamp) + " to current time of " + str(timestamp))
			if (msg_timestamp == (vector_timestamp + 1) or (msg_timestamp == vector_timestamp)):
				message_queue.remove(message)
				msg = message
				break

	if (msg):
		deliver_message(message[0], message[1], message[2])


if __name__ == "__main__":
	signal.signal(signal.SIGINT, lambda x,y: sys.exit(0))
	if (len(sys.argv) != 2):
		print("python " + sys.argv[0] + " <#>")
	else:
		main(sys.argv[1:])