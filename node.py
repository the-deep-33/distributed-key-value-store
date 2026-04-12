from dataclasses import dataclass, field
from config import CLUSTER
from models import LogEntry, RequestVote, VoteResponse, AppendEntries, AppendEntriesResponse
import argparse
import socket
from threading import Thread
import json
from dataclasses import asdict

@dataclass
class Node:
    """All the elements that a node needs, regardless if it is a leader or a follower"""
    id: int
    leader_id: int = None
    current_term: int = 0
    state: str = "follower"
    commit_index: int = 0
    last_applied: int = 0
    log: list = field(default_factory = list)
    store: dict = field(default_factory = dict)
    next_index: dict = field(default_factory = dict)
    voted_for: int = None

    def start(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ADRESA = CLUSTER[self.id]
        self.server.bind(ADRESA)
        self.server.listen()
        
        while True:
            conn, addr = self.server.accept()
            t = Thread(target = self.handle_connection, args = (conn, addr), daemon = True)
            t.start()

    def handle_connection(self, conn, addr):
        try:
            while True:
                msg_len = int(conn.recv(128).decode())
                msg = json.loads(conn.recv(msg_len).decode())
                self.process_msg(msg)
        except:
            conn.close()

    def send_message(self, conn, msg):
        name = type(msg).__name__
        msg = asdict(msg)
        msg["type"] = name
        msg = json.dumps(msg)
        length = len(msg)
        length_to_send = (str(length) + (128 - len(str(length))) * " ").encode()
        conn.send(length_to_send)
        conn.send(msg.encode())

    def process_msg(self, msg):
        msg_type = msg["type"]
        print(msg_type)
        match msg_type:
            case "RequestVote":
                pass
            case "VoteResponse":
                pass
            case "AppendEntries":
                pass
            case "AppendEntriesResponse":
                pass
            case "ClientCommand":
                pass



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type = int, help = "ID of the node you want to create")
    args = parser.parse_args()
    node = Node(id=args.id)
    print(f"Created node with id: {node.id}, state = {node.state}, term = {node.current_term}")
    node.start()
