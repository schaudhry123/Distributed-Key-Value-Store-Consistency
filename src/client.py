import sys
from threading import Thread, Lock
import socket
import pickle
import datetime
import time
import random
import signal
import Queue

servers = []
server_id = 0
client_sockets = [] # client_sockets[i] = { 'id' = 1, 'socket'='', 'v_timestamp' = 1}
vector_timestamps = []
message_queue = []
mutex = Lock()

# Ran with commands "python basicMessages.py <#process_id>"
def main(argv):
	global server_id
	server_id = parse_file(argv[0])

	# If process id could not be found
	if (server_id == -1):
		print("Did not find server id " + argv[0] + " in config file.")
		print("Exiting client.")
	else:
		# Create server and client threads
		try:
			client_thread = Thread(target=setup_client, args = ())
			client_thread.daemon = True
			client_thread.start()

			# Find an elegant way to keep main running until done
			while True:
				time.sleep(100)
		except:
			print("Failed to start client.")


'''
Parses the config file for data about min/max delay and all processes info
'''
def parse_file(server_num):
	counter = 0
	num_lines = 0
	index = -1

	# Count number of lines
	with open('../configs/config.txt') as f:
		for line in f:
			num_lines++
	# Wrap around to the lowest server if no higher servers
	server_num = server_num % (num_lines-1)

	# Find the server information
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
	return index

'''
Sets up the client for the current process, reading input from the command line and unicasting/multicasting it out
'''
def setup_client():
	global server_id
	server = servers[server_id]
	while True:
		user_input = raw_input('')

		if (user_input == "exit"):
			break;

		if (user_input):
			input_split = user_input.split()
			message = ''

			process_id = int(server[0])

			valid = False
			if (len(input_split) > 1 and input_split[1].isalpha()):
				if (len(input_split) == 3 and input_split[0] == "put" and input_split[2].isdigit()):
					message = "p" + input_split[1] + input_split[2]
					valid = True
				elif (input_split[0] == "get"):
					message = "g" + input_split[1]
					valid = True
			elif (input_split[0] == "delay"):
				# SLEEP ON SOME SHIT
				valid = True
			elif (user_input == "dump"):
				message = "d"
				valid = True

			if (not valid):
				print("Invalid command. Valid commands are:")
				print("put <var> <value>, get <var>, delay <ms>, dump")

			if (message):
				# If server is up, send message. Else connect to another server and kepe trying
				destination = int(server[0])
				sent_success = unicast_send(message, "client", server)
				tries = 0 # To prevent client from spamming a bunch of servers
				while (sent_success == 0 and tries < 5):
					server_id = parse_file(int(server[0])+1)
					unicast_send(message, "client", server)
					tries += 1

				# Get response
				response =


	print("Exiting client")
	for i in range(len(client_sockets)):
		client_sockets[i]['socket'].close()

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
Unicasts a message to the specified process given the message, current process info, and the specified process id
'''
def unicast_send(message, process, destinationInfo):
	# If the id is already in client_sockets
	for i in range(len(client_sockets)):
		# print("Have already connected to " + str(destinationInfo))
		if (destinationInfo[0] == client_sockets[i]['id']):
			return send_message(message, process, client_sockets[i], timestamp)

	# Else open up a new socket to the process
	if (create_connection(destinationInfo)):
		return send_message(message, process, client_sockets[len(client_sockets)-1], timestamp)

'''
Sends a message object to a process given the destination process info, the message, and the source process id
'''
def send_message(message, source, destination_process, timestamp):
	msg = {
			'message': message,
			'source': source,
			'destination': destination_process['id'],
			'timestamp': timestamp,
			'server': False
	}
	seralized_message = pickle.dumps(msg, -1)
	print("Sent " + message + " to process " + str(destination_process['id']) + ", system time is " + str(datetime.datetime.now()).split(".")[0])
	return destination_process['socket'].sendall(seralized_message)

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

		# print("Receiving " + message + " w/ timestamp " + str(timestamp) + " with own time " + str(vector_timestamps[1]))

		receive_message(message, source, destination, timestamp)

		# receive_thread = Thread(target=receive_message, args = (message, source, destination, client_timestamp, True))
		# receive_thread.daemon = True
		# receive_thread.start()


def receive_message(message, source, destination, timestamp):

	# mutex.acquire()
	# print("Received " + message + " from process " + str(source) + " with msg_timestamp = " + str(timestamp))
	# If the message should be delivered, deliver it
	if (delay_message(message, source, destination, timestamp)):
		deliver_message(message, source, destination)
		# print("After deliver: Process timestamp = " + str(timestamp) + ", client timestamp = " + str(client_timestamp))

	# mutex.release()

def deliver_message(message, source, destination):

	mutex.acquire()
	# If not the sequencer, update the timestamp. Else multicast message out if not sent from itself
	if (destination != 0):
		vector_timestamps[0] += 1
	mutex.release()

	print("Delivered " + message + " from process " + str(source) + ", system time is " + str(datetime.datetime.now()).split(".")[0])

	if (destination == 0):
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

	# if (source != destination):
	time.sleep((random.uniform(min_delay, max_delay)/1000.0))	# Random network delay

	if (destination == 0):
		return True

	mutex.acquire()
	current_time = vector_timestamps[0]
	mutex.release()

	# Deliver the message
	if (timestamp == (current_time + 1) or timestamp == current_time):
		return True

	message_queue.append((message, source, destination, timestamp))

	return False

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
			msg_timestamp = int(message[3])
			# print("Comparing msg w/ " + str(msg_timestamp) + " to current time of " + str(timestamp))
			if (msg_timestamp == (timestamp + 1) or (msg_timestamp == timestamp)):
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