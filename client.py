import socket
import json
from config import CLUSTER
from threading import Thread
import random

def start():
    id = input("Enter the node id: ")
    id = int(id)
    socket_params = CLUSTER[id]
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(socket_params)
    handle_requests(client)


def handle_requests(client):
    user_input = input("Enter the command that you wish to execute: \n")
    msg_split = user_input.split(" ")

    msg = dict()

    msg["type"] = "ClientCommand"
    msg["command"] = msg_split[0]
    msg["key"] = msg_split[1]

    if msg["command"]== "SET":
        msg["value"] = msg_split[2]

    msg_json = json.dumps(msg)

    length_msg_json = len(msg_json)
    padding = b" " * (128 - len(str(length_msg_json)))

    header = str(length_msg_json).encode() + padding

    client.sendall(header)
    client.sendall(msg_json.encode())

    response_header = client.recv(128)
    response = client.recv(int(response_header.decode().strip()))

    print(response)

if __name__ == "__main__":
    start()