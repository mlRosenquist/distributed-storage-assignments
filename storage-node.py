"""
Aarhus University - Distributed Storage course - Lab 4

Storage Node
"""
import zmq
from models import messages_pb2

import sys
import os

from utils import random_string, write_file, is_raspberry_pi

MAX_CHUNKS_PER_FILE = 10

# Read the folder name where chunks should be stored from the first program argument
# (or use the current folder if none was given)
data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"
if data_folder != "./":
    # Try to create the folder  
    try:
        os.mkdir('./'+data_folder)
    except FileExistsError as _:
        # OK, the folder exists 
        pass
print("Data folder: %s" % data_folder)

# Check whether the node has an id. If it doesn't, generate one and save it to disk.
try:
    with open(data_folder+'/.id', "r") as id_file:
        node_id = id_file.read()
        print("ID read from file: %s" % node_id)

except FileNotFoundError:
    # This is OK, this must be the first time the node was started
    node_id = random_string(8)
    # Save it to file for the next start
    with open(data_folder+'/.id', "w") as id_file:
        id_file.write(node_id)
        print("New ID generated and saved to file: %s" % node_id)

if is_raspberry_pi():
    # On the Raspberry Pi: ask the user to input the last segment of the server IP address
    server_address = input("Server address: 192.168.0.___ ")
    pull_address = "tcp://192.168.0."+server_address+":5557"
    sender_address = "tcp://192.168.0."+server_address+":5558"
    subscriber_address = "tcp://192.168.0."+server_address+":5559"
    worker_subscriber_address = "tcp://192.168.0."+server_address+":5562"
    worker_sender_address = "tcp://192.168.0."+server_address+":5563"
else:
    # On the local computer: use localhost
    pull_address = "tcp://localhost:5557"
    push_address = "tcp://localhost:5558"
    subscriber_address = "tcp://localhost:5559"
    worker_subscriber_address = "tcp://localhost:5562"
    worker_sender_address = "tcp://localhost:5563"

context = zmq.Context()
# Socket to receive Store Chunk messages from the controller
receiver = context.socket(zmq.PULL)
receiver.connect(pull_address)
print("Listening on "+ pull_address)
# Socket to send results to the controller
sender = context.socket(zmq.PUSH)
sender.connect(push_address)
# Socket to receive Get Chunk messages from the controller
subscriber = context.socket(zmq.SUB)
subscriber.connect(subscriber_address)
# Receive every message (empty subscription)
subscriber.setsockopt(zmq.SUBSCRIBE, b'')

# Socket to receive worker request messages from the controller
worker_subscriber = context.socket(zmq.SUB)
worker_subscriber.connect(worker_subscriber_address)
# Receive messages destined for this node
worker_subscriber.setsockopt(zmq.SUBSCRIBE, node_id.encode('UTF-8'))
# Socket to send repair results to the controller
worker_sender = context.socket(zmq.PUSH)
worker_sender.connect(worker_sender_address)


# Use a Poller to monitor three sockets at the same time
poller = zmq.Poller()
poller.register(receiver, zmq.POLLIN)
poller.register(subscriber, zmq.POLLIN)
poller.register(worker_subscriber, zmq.POLLIN)

while True:
    try:
        # Poll all sockets
        socks = dict(poller.poll())
    except KeyboardInterrupt:
        break
    pass

    # At this point one or multiple sockets may have received a message

    if receiver in socks:
        # Incoming message on the 'receiver' socket where we get tasks to store a chunk
        msg = receiver.recv_multipart()
        # Parse the Protobuf message from the first frame
        task = messages_pb2.storedata_request()
        task.ParseFromString(msg[0])

        # The data starts with the second frame, iterate and store all frames
        for i in range(0, len(msg)-1):
            data = msg[1+i]

            print('Chunk to save: %s, size: %d bytes' %
                  (task.filename + "." + str(i), len(data)))

            # Store the chunk with the given filename
            chunk_local_path = data_folder+'/'+task.filename+"."+str(i)
            write_file(data, chunk_local_path)
            print("Chunk saved to %s" % chunk_local_path)

        # Send response (just the file name)
        sender.send_string(task.filename)

    if subscriber in socks:
        # Incoming message on the 'subscriber' socket where we get retrieve requests
        msg = subscriber.recv()
        
        # Parse the Protobuf message from the first frame
        task = messages_pb2.getdata_request()
        task.ParseFromString(msg)

        filename = task.filename
        print("Data chunk request: %s" % filename)

        # Try to load all fragments with this name
        # First frame is the filename
        frames = [bytes(filename, 'utf-8')]
        # Subsequent frames will contain the chunks' data
        for i in range(0, MAX_CHUNKS_PER_FILE):
            try:
                with open(data_folder+'/'+filename+"."+str(i), "rb") as in_file:
                    print("Found chunk %s, sending it back" % filename)
                    # Add chunk as a new frame
                    frames.append(in_file.read())

            except FileNotFoundError:
                # This is OK here
                break

        #Only send a result if at least one chunk was found
        if(len(frames)>1):
            sender.send_multipart(frames)

    if worker_subscriber in socks:
        print("hello")
#
