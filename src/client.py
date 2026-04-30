import socket
import json
from config import CLUSTER
from threading import Thread
import random
import time

def start():
    id = input("Enter the node id: ")
    id = int(id)
    socket_params = CLUSTER[id]
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(socket_params)
    while True:
        try:
            client = handle_requests(client)
        except:
            print("Connection broke, connecting to a random node")
            try:
                client.close()
            except:
                pass
            while True:
                try:
                    new_node = random.randint(1, len(CLUSTER))
                    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    client.connect(CLUSTER[new_node])
                    print(f"Connected to the node: {new_node}")
                    break
                except:
                    print(f"Node {new_node} is unavailable, trying new one...")
                    time.sleep(0.25)


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
    response = json.loads(client.recv(int(response_header.decode().strip())).decode())

    if response["status"] == "redirect":
        leader_id = response["leader_id"]
        print(f"Redirecting to node: {leader_id}")
        client.close()
        new_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        new_client.connect(CLUSTER[leader_id])
        return new_client


    print(response)
    return client



if __name__ == "__main__":
    start()