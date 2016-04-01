#!/usr/bin/env python

# Found on StackExchange (credit http://stackoverflow.com/users/703144/berto)
# simply provides .csv logs for the .html visualization pages to load at the default address of
# 	http://localhost:8000/<logfile>.csv

import collections
import os
import socket
import SimpleHTTPServer
import threading

IP = "127.0.0.1"
PORT = 1234

class MyHTTPRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
  def end_headers(self):
    self.send_my_headers()
    SimpleHTTPServer.SimpleHTTPRequestHandler.end_headers(self)

  def send_my_headers(self):
    self.send_header("Access-Control-Allow-Origin", "*")

  def log_message(self, format, *args):
    return

def saveLog(logID, log):
  with open(os.path.join('logs', logID + ".csv"), "w") as log_file:
    log_file.write("Client,Request,Key,Timestamp,Action,Value\n")
    for k in sorted(log):
      log_file.write(log[k])

def updateLog():
  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  sock.bind((IP, PORT))

  logs = {}

  while True:
    data, addr = sock.recvfrom(128)
    data = data.split('\n')[0]
    CMD = data.split(',', 1)[0]
    MSG = data.split(',', 1)[1]
    if CMD == 'START':
      logID = MSG
      logs[logID] = {}
    else:
      MSG += '\n'
      timestamp = int(MSG.split(',')[3])
      logs[logID][timestamp] = MSG
      saveLog(logID, logs[logID])

def loadLog():
  SimpleHTTPServer.test(HandlerClass=MyHTTPRequestHandler)

if __name__ == '__main__':
  update_thread = threading.Thread(target=updateLog, args=())
  load_thread = threading.Thread(target=loadLog, args=())
  update_thread.start()
  load_thread.start()
  update_thread.join()
  load_thread.join()

