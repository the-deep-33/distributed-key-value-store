from dataclasses import dataclass, field
from config import CLUSTER
from models import LogEntry, RequestVote, VoteResponse, AppendEntries, AppendEntriesResponse
import argparse
import socket
from threading import Thread, Event
import json
from dataclasses import asdict
import random

@dataclass
class Node:
    """All the elements that a node needs, regardless if it is a leader or a follower"""
    id: int
    heartbeat_event: Event = field(default_factory = Event)
    leader_id: int = None
    current_term: int = 0
    state: str = "follower"
    commit_index: int = 0
    last_applied: int = 0
    log: list = field(default_factory = list)
    store: dict = field(default_factory = dict)
    next_index: dict = field(default_factory = dict)
    voted_for_me_total: int = 0
    voted_for: int = None

    def start(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ADRESA = CLUSTER[self.id]
        self.server.bind(ADRESA)
        self.server.listen()

        election = Thread(target = self.election_loop, daemon = True)
        election.start()
        
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
                self.vote(msg)
            case "VoteResponse":
                pass
            case "AppendEntries":
                pass
            case "AppendEntriesResponse":
                pass
            case "ClientCommand":
                pass

    def election_loop(self):
        while True:
            timeout = random.uniform(0.15, 0.3)
            heartbeat_received = self.heartbeat_event.wait(timeout)
            if heartbeat_received:
                self.heartbeat_event.clear()
                continue
            else:
                self.current_term += 1
                if self.log:
                    last_log_index = self.log[-1].index
                    last_log_term = self.log[-1].term
                else:
                    last_log_index = 0
                    last_log_term = 0

                self.state = "candidate"
                self.voted_for = self.id
                req = RequestVote(node_id = self.id, term = self.current_term, last_log_index = last_log_index, last_log_term = last_log_term)

                for i in CLUSTER:
                    if i != self.id:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.connect(CLUSTER[i])
                        self.send_message(sock, req)

        def vote(self, msg):
            my_vote = VoteResponse(node_id=self.id, term=self.current_term, response=False)
            vote_term = msg["term"]
            vote_last_log_index = msg["last_log_index"]
            vote_last_log_term = msg["last_log_term"]
            if vote_term < self.current_term:
                my_vote.response = False
            elif vote_term == self.current_term:
                if self.voted_for == None:
                    self.voted_for = msg["node_id"]
                    my_vote.response = True
                else:
                    my_vote.response = False
            else:
                self.state = "follower"
                self.current_term = vote_term
                self.voted_for = msg["node_id"]
                my_vote.response = True

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(CLUSTER[msg["node_id"]])
            self.send_message(sock, my_vote)






if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type = int, help = "ID of the node you want to create")
    args = parser.parse_args()
    node = Node(id=args.id)
    print(f"Created node with id: {node.id}, state = {node.state}, term = {node.current_term}")
    node.start()
