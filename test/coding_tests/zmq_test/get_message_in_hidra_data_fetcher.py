import zmq
import json
import os
import tempfile

ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
current_pid = 12345

out_endpoint = "ipc://{}/{}_{}".format(ipc_dir, current_pid, "out")

context = zmq.Context()
socket = context.socket(zmq.PULL)
socket.connect(out_endpoint)

message = socket.recv_multipart()
print "Received - {}".format(message)

socket.close()
context.destroy()
