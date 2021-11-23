import json
import zmq
import time
import io
import logging

from flask import Flask, make_response, g, request, send_file
from erasure_codes import  reedsolomon
from models.file import File
from repositories import file_repository
from utils import is_raspberry_pi

# Initiate ZMQ sockets
context = zmq.Context()

# Socket to send tasks to Storage Nodes
send_task_socket = context.socket(zmq.PUSH)
send_task_socket.bind("tcp://*:5557")

# Socket to receive messages from Storage Nodes
response_socket = context.socket(zmq.PULL)
response_socket.bind("tcp://*:5558")

# Publisher socket for data request broadcasts
data_req_socket = context.socket(zmq.PUB)
data_req_socket.bind("tcp://*:5559")

worker_socket = context.socket(zmq.PUB)
worker_socket.bind("tcp://*:5562")

worker_response_socket = context.socket(zmq.PULL)
worker_response_socket.bind("tcp://*:5563")

# Wait for all workers to start and connect. 
time.sleep(1)
print("Listening to ZMQ messages on tcp://*:5558 and tcp://*:5561")


# Instantiate the Flask app (must be before the endpoint functions)
app = Flask(__name__)


@app.route('/files',  methods=['GET'])
def list_files():
    files = file_repository.get_files()
    # Convert files from sqlite3.Row object (which is not JSON-encodable) to 
    # a standard Python dictionary simply by casting
    files = list(map(lambda f: f.to_dict(), files))
    return make_response({"files": json.dumps(files)})


@app.route('/files/<string:file_id>',  methods=['GET'])
def download_file(file_id):

    file = file_repository.get_file(file_id)

    print(f"File requested: {file.fileName}")
    
    # Parse the storage details JSON string
    storage_details = json.loads(file.storage_details)

    if file.storage_mode == 'erasure_coding_rs':
        
        coded_fragments = storage_details['coded_fragments']
        max_erasures = storage_details['max_erasures']

        file_data = reedsolomon.get_file(
            coded_fragments,
            max_erasures,
            file.size,
            data_req_socket, 
            response_socket
        )

    return send_file(io.BytesIO(file_data), mimetype=file.content_type)


@app.route('/files_mp', methods=['POST'])
def add_files_multipart():
    # Flask separates files from the other form fields
    payload = request.form
    files = request.files
    
    # Make sure there is a file in the request
    if not files or not files.get('file'):
        logging.error("No file was uploaded in the request!")
        return make_response("File missing!", 400)
    
    # Reference to the file under 'file' key
    file = files.get('file')
    # The sender encodes a the file name and type together with the file contents
    filename = file.filename
    content_type = file.mimetype
    # Load the file contents into a bytearray and measure its size
    data = bytearray(file.read())
    size = len(data)
    print("File received: %s, size: %d bytes, type: %s" % (filename, size, content_type))
    
    # Read the requested storage mode from the form (default value: 'raid1')
    storage_mode = payload.get('storage', 'erasure_coding_rs')
    print("Storage mode: %s" % storage_mode)

    if storage_mode == 'erasure_coding_rs':
        # Reed Solomon code
        # Parse max_erasures (everything is a string in request.form, 
        # we need to convert to int manually), set default value to 1
        max_erasures = int(payload.get('max_erasures', 1))
        print("Max erasures: %d" % (max_erasures))
        
        # Store the file contents with Reed Solomon erasure coding
        fragment_names = reedsolomon.store_file(data, max_erasures, send_task_socket, response_socket)

        storage_details = {
            "coded_fragments": fragment_names,
            "max_erasures": max_erasures
        }
        
    else:
        logging.error("Unexpected storage mode: %s" % storage_mode)
        return make_response("Wrong storage mode", 400)

    # Insert the File record in the DB

    file = File(fileName=filename,
                size=size, content_type=content_type,
                storage_mode=storage_mode,
                storage_details=json.dumps(storage_details))

    file_repository.add_file(file)
    file = file.to_dict()
    return make_response({"id": file['id'] }, 201)


@app.errorhandler(500)
def server_error(e):
    logging.exception("Internal error: %s", e)
    return make_response({"error": str(e)}, 500)

# Start the Flask app (must be after the endpoint functions) 
host_local_computer = "localhost" # Listen for connections on the local computer
host_local_network = "0.0.0.0" # Listen for connections on the local network
app.run(host=host_local_network if is_raspberry_pi() else host_local_computer, port=9000)
