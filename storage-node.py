
import time
import zmq
import erasure_codes.reedsolomon
from erasure_codes import reedsolomon
from models import messages_pb2
import sys
import os

from utils import random_string, write_file, is_raspberry_pi

#region Folder initialization
MAX_CHUNKS_PER_FILE = 10

# Read the folder name where chunks should be stored from the first program argument
# (or use the current folder if none was given)
node_no = sys.argv[1]

if not (node_no == "0" or node_no == "1" or node_no == "2" or node_no == "3"):
    raise Exception("Node number needs to be between 1-4")

# Try to create the folder
try:
    os.mkdir('./'+ node_no)
except FileExistsError as _:
    # OK, the folder exists
    pass
print("Data folder: %s" % node_no)

# Check whether the node has an id. If it doesn't, generate one and save it to disk.
try:
    with open(node_no+'/.id', "r") as id_file:
        node_id = id_file.read()
        print("ID read from file: %s" % node_id)

except FileNotFoundError:
    # This is OK, this must be the first time the node was started
    node_id = random_string(8)
    # Save it to file for the next start
    with open(node_no+'/.id', "w") as id_file:
        id_file.write(node_id)
        print("New ID generated and saved to file: %s" % node_id)
#endregion

#region ZMQ address initialization
other_storage_node_addresses = []
storage_node_pull_addresses = dict()
storage_node_response_push_addresses = dict()


if is_raspberry_pi():
    # Get addresses storage nodes
    node_address = int(input("Storage Node address: 192.168.0.___ "))

    # Addresses to talk to communicate with lead node
    lead_address = input("Server address: 192.168.0.___ ")
    lead_pull_address = "tcp://192.168.0." + lead_address + ":5555"
    lead_push_address = "tcp://192.168.0." + lead_address + ":5556"
    lead_subscriber_address = "tcp://192.168.0." + lead_address + ":5557"

    # Addresses to talk to communicate with other storage nodes
    for i in range(3):
        other_storage_node_addresses.append(int(input("Storage Node address: 192.168.0.___ ")))

    # Setup addresses to pull and push to other storage nodes
    for address in other_storage_node_addresses:
        storage_node_pull_addresses[address] = f'tcp://192.168.0.{address}:5560'
        storage_node_response_push_addresses[address] = f'tcp://192.168.0.{address}:5570'

    storage_node_push_socket_address = "tcp://*:5560"
    storage_node_response_pull_socket_address = "tcp://*:5570"

else:
    # On the local computer: use localhost
    node_address = int(node_no)

    # Addresses to talk to communicate with lead node
    lead_pull_address = "tcp://localhost:5555"
    lead_push_address = "tcp://localhost:5556"
    lead_subscriber_address = "tcp://localhost:5557"

    # On local host we instead of reading the address from user input we just set it from 0-4
    for i in range(4):
        other_storage_node_addresses.append(i)

    # We dont want to listen to our own socket
    other_storage_node_addresses.remove(int(node_no))

    # Setup addresses to pull and push to other storage nodes
    for address in other_storage_node_addresses:
        storage_node_pull_addresses[address] = f'tcp://localhost:556{address}'
        storage_node_response_push_addresses[address] = f'tcp://localhost:557{address}'

    storage_node_push_socket_address = f"tcp://*:556{node_no}"
    storage_node_response_pull_socket_address = f"tcp://*:557{node_no}"
#endregion

#region ZMQ socket initialization
storage_node_pull_sockets = dict()
storage_node_response_push_sockets = dict()

context = zmq.Context()
# Socket to receive Store Chunk messages from the controller
lead_receiver = context.socket(zmq.PULL)
lead_receiver.connect(lead_pull_address)

# Socket to send results to the controller
lead_sender = context.socket(zmq.PUSH)
lead_sender.connect(lead_push_address)

# Socket to receive Get Chunk messages from the controller
lead_subscriber = context.socket(zmq.SUB)
lead_subscriber.connect(lead_subscriber_address)

# Receive every message (empty subscription)
lead_subscriber.setsockopt(zmq.SUBSCRIBE, b'')

# Create push sockets to send messages to other storage nodes
storage_node_push_socket = context.socket(zmq.PUSH)
storage_node_push_socket.bind(storage_node_push_socket_address)

# Each node gets a pull socket to receive responses from tasks
storage_node_response_pull_socket = context.socket(zmq.PULL)
storage_node_response_pull_socket.bind(storage_node_response_pull_socket_address)

for address in other_storage_node_addresses:
    storage_node_pull_sockets[address] = context.socket(zmq.PULL)
    storage_node_pull_sockets[address].connect(storage_node_pull_addresses[address])
    storage_node_response_push_sockets[address] = context.socket(zmq.PUSH)
    storage_node_response_push_sockets[address].connect(storage_node_response_push_addresses[address])

# Let nodes connect
time.sleep(1)
#endregion

#region helper methods
def handle_store_data_req(msg, response_socket):
    # Parse the Protobuf message from the first frame
    task = messages_pb2.storedata_request()
    task.ParseFromString(msg[1])

    # The data starts with the second frame, iterate and store all frames
    for i in range(0, len(msg) - 2):
        data = msg[2 + i]

        print('Chunk to save: %s, size: %d bytes' %
              (task.filename + "." + str(i), len(data)))

        # Store the chunk with the given filename
        chunk_local_path = node_no + '/' + task.filename + "." + str(i)
        write_file(data, chunk_local_path)
        print("Chunk saved to %s" % chunk_local_path)

    # Send response (just the file name)
    response_socket.send_string(task.filename)
#endregion

# Use a Poller to monitor all sockets at the same time
poller = zmq.Poller()
poller.register(lead_receiver, zmq.POLLIN)
poller.register(lead_subscriber, zmq.POLLIN)
poller.register(storage_node_pull_sockets[other_storage_node_addresses[0]], zmq.POLLIN)
poller.register(storage_node_pull_sockets[other_storage_node_addresses[1]], zmq.POLLIN)
poller.register(storage_node_pull_sockets[other_storage_node_addresses[2]], zmq.POLLIN)

while True:
    try:
        # Poll all sockets
        socks = dict(poller.poll())
    except KeyboardInterrupt:
        break
    pass

    # At this point one or multiple sockets may have received a message

    # Task received from lead node
    if lead_receiver in socks:
        msg = lead_receiver.recv_multipart()
        header = messages_pb2.header()
        header.ParseFromString(msg[0])

        if header.request_type == messages_pb2.WORKER_STORE_FILE_REQ:
            print("Starting to store file on the storage nodes")
            task = messages_pb2.worker_store_file_request()
            task.ParseFromString(msg[1])
            data = msg[2]

            # Store the file contents with Reed Solomon erasure coding
            tasks, fragments = reedsolomon.get_store_file_tasks(bytearray(data), task.max_erasures)

            fragment_names = list(map(lambda x: x.filename, tasks))
            # Store a fragment on current node
            task = tasks.pop()
            fragment = fragments.pop()
            write_file(fragment, f'{node_no}/{task.filename}')
            print(f"Stored {task.filename} on this node")

            print("Sending store data requests to other nodes")
            header = messages_pb2.header()
            header.request_type = messages_pb2.STORE_FRAGMENT_DATA_REQ
            for task, fragment in zip(tasks, fragments):
                task.node_return_address = node_address
                storage_node_push_socket.send_multipart([
                    header.SerializeToString(),
                    task.SerializeToString(),
                    fragment
                ])
            print("Awaiting responses from other nodes")
            for task_nbr in range(3):
                resp = storage_node_response_pull_socket.recv()
                print('Received: %s' % resp)

            print("File stored on nodes")

            print("Sending response to lead node")
            task = messages_pb2.worker_store_file_response()
            print(fragment_names)
            task.fragments[:] = fragment_names

            lead_sender.send_multipart([
                task.SerializeToString()
            ])
        elif header.request_type == messages_pb2.STORE_FRAGMENT_DATA_REQ:
            handle_store_data_req(msg, lead_sender)
        else:
            raise NotImplementedError("Unknown header type")

    # Task received from lead node
    if lead_subscriber in socks:
        # Incoming message on the 'subscriber' socket where we get retrieve requests
        msg = lead_subscriber.recv_multipart()
        header = messages_pb2.header()
        header.ParseFromString(msg[0])

        if header.request_type == messages_pb2.FRAGMENT_DATA_REQ:
            task = messages_pb2.getdata_request()
            task.ParseFromString(msg[1])
            print("Data chunk request: %s" % task.filename)

            # Try to load all fragments with this name
            # First frame is the filename
            frames = [bytes(task.filename, 'utf-8')]
            # Subsequent frames will contain the chunks' data
            for i in range(0, MAX_CHUNKS_PER_FILE):
                try:
                    with open(node_no + '/' + task.filename + "." + str(i), "rb") as in_file:
                        print("Found chunk %s, sending it back" % task.filename)
                        # Add chunk as a new frame
                        frames.append(in_file.read())

                except FileNotFoundError:
                    # This is OK here
                    break

            # Only send a result if at least one chunk was found
            if (len(frames) > 1):
                lead_sender.send_multipart(frames)





        else:
            raise NotImplementedError("Unknown header type")

    # Task received from other storage node
    if storage_node_pull_sockets[other_storage_node_addresses[0]] in socks:
        msg = storage_node_pull_sockets[other_storage_node_addresses[0]].recv_multipart()
        header = messages_pb2.header()
        header.ParseFromString(msg[0])

        if header.request_type == messages_pb2.STORE_FRAGMENT_DATA_REQ:
            task = messages_pb2.storedata_request()
            task.ParseFromString(msg[1])
            handle_store_data_req(msg, storage_node_response_push_sockets[task.node_return_address])


    # Task received from other storage node
    if storage_node_pull_sockets[other_storage_node_addresses[1]] in socks:
        msg = storage_node_pull_sockets[other_storage_node_addresses[1]].recv_multipart()
        header = messages_pb2.header()
        header.ParseFromString(msg[0])

        if header.request_type == messages_pb2.STORE_FRAGMENT_DATA_REQ:
            task = messages_pb2.storedata_request()
            task.ParseFromString(msg[1])
            handle_store_data_req(msg, storage_node_response_push_sockets[task.node_return_address])

    # Task received from other storage node
    if storage_node_pull_sockets[other_storage_node_addresses[2]] in socks:
        msg = storage_node_pull_sockets[other_storage_node_addresses[2]].recv_multipart()
        header = messages_pb2.header()
        header.ParseFromString(msg[0])

        if header.request_type == messages_pb2.STORE_FRAGMENT_DATA_REQ:
            task = messages_pb2.storedata_request()
            task.ParseFromString(msg[1])
            handle_store_data_req(msg, storage_node_response_push_sockets[task.node_return_address])
#