from dataclasses import dataclass, field
from config import CLUSTER
from models import LogEntry, RequestVote, VoteResponse, AppendEntries, AppendEntriesResponse
import argparse
import socket
from threading import Thread, Event, Lock
import json
from dataclasses import asdict
import random
import time

@dataclass
class Node:
    """All the elements that a node needs, regardless if it is a leader or a follower"""
    id: int
    heartbeat_event: Event = field(default_factory = Event)
    lock: Lock = field(default_factory = Lock)
    leader_id: int = None
    current_term: int = 0
    state: str = "follower"
    commit_index: int = 0
    last_applied: int = 0
    log: list = field(default_factory = list)
    store: dict = field(default_factory = dict)
    next_index: dict = field(default_factory = dict)
    match_index: dict = field(default_factory = dict)
    voted_for_me_total: int = 0
    voted_for: int = None

    def start(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ADRESA = CLUSTER[self.id]
        self.server.bind(ADRESA)
        self.server.listen()

        election = Thread(target = self.election_loop, daemon = True)
        election.start()

        heartbeat = Thread(target=self.heartbeat_loop, daemon=True)
        heartbeat.start()

        apply_loop = Thread(target = self.handle_commits, daemon = True)
        apply_loop.start()
        
        while True:
            conn, addr = self.server.accept()
            t = Thread(target = self.handle_connection, args = (conn, addr), daemon = True)
            t.start()

    def handle_commits(self):
        while True:
            with self.lock:
                while self.last_applied < self.commit_index:
                    print(f"last_applied = {self.last_applied}, commit_index = {self.commit_index}")
                    entry = self.log[self.last_applied].command
                    
                    key = entry["key"]

                    if entry["type"] == "SET":
                        self.store[key] = entry["value"]
                    
                    elif entry["type"] == "DEL":
                        try:
                            del self.store[key]
                        except:
                            # If there is no such key in the dictionary
                            pass

                    self.last_applied += 1
            time.sleep(0.01)


    def handle_connection(self, conn, addr):
        try:
            header = self.recv_exact(conn, 128)
            if header is None:
                return
            msg_len = int(header.decode().strip())
            buffer = self.recv_exact(conn, msg_len)
            if buffer is None:
                return
            msg = json.loads(buffer.decode())
            self.process_msg(msg, conn, addr)
        except Exception as e:
            print(f"Node {self.id} handle_connection error: {e}")
        finally:
            conn.close()

    def recv_exact(self, conn, n):
        buff = b""

        while len(buff) < n:
            chunk = conn.recv(n - len(buff))
            if chunk:
                buff += chunk
            else:
                return None

        return buff

    def send_message(self, conn, msg):
        name = type(msg).__name__
        msg = asdict(msg)
        msg["type"] = name
        msg = json.dumps(msg)
        length = len(msg)
        length_to_send = (str(length) + (128 - len(str(length))) * " ").encode()
        conn.sendall(length_to_send)
        conn.sendall(msg.encode())

    def process_msg(self, msg, conn, addr):
        msg_type = msg["type"]
        match msg_type:
            case "RequestVote":
                self.vote(msg)
            case "VoteResponse":
                with self.lock:
                    vote_term = msg["term"]
                    if vote_term > self.current_term:
                        self.current_term = vote_term
                        self.state = "follower"
                        self.voted_for = None
                        return
                    if vote_term < self.current_term:
                        return
                    vote_response = msg["response"]
                    if vote_response:
                        self.voted_for_me_total += 1
                    if self.voted_for_me_total > len(CLUSTER) // 2 and self.state == "candidate":
                        for i in CLUSTER:
                            if i != self.id:
                                self.next_index[i] = len(self.log) + 1
                                self.match_index[i] = 0
                        self.state = "leader"
                        self.leader_id = self.id
                        self.heartbeat_event.set()
                        print(f"Node {self.id} je postao LIDER u term-u {self.current_term}")
            case "AppendEntries":
                if msg["term"] >= self.current_term:
                    with self.lock:
                        self.heartbeat_event.set()
                        self.state = "follower"
                        self.leader_id = msg["leader_id"]
                        self.current_term = msg["term"]
                        append_entries_response = AppendEntriesResponse(node_id = self.id, success = True)
                        

                        if msg["entries"]:
                            if self.log:
                                my_index = msg["prev_log_index"]
                                my_term = msg["prev_log_term"]
                                if len(self.log) < my_index:
                                    append_entries_response.success = False
                                    append_entries_response.last_log_index = len(self.log)

                                elif self.log[my_index - 1].term != my_term:
                                    append_entries_response.success = False
                                    append_entries_response.last_log_index = my_index - 1

                                elif len(self.log) > my_index:
                                    self.log = self.log[:my_index]
                                    append_entries_response.success = True

                                    dictionary_logs = msg["entries"]
                                    for l in dictionary_logs:
                                        self.log.append(LogEntry(**l))

                                    append_entries_response.last_log_index = self.log[-1].index

                                    if msg["leader_commit"] > self.commit_index:
                                        self.commit_index = min(msg["leader_commit"], len(self.log))

                                else:
                                    append_entries_response.success = True
                                    append_entries_response.last_log_index = self.log[-1].index
                                    dictionary_logs = msg["entries"]
                                    for l in dictionary_logs:
                                        self.log.append(LogEntry(**l))

                                    if msg["leader_commit"] > self.commit_index:
                                        self.commit_index = min(msg["leader_commit"], len(self.log))
                                
                            else:
                                my_index = msg["prev_log_index"]
                                if my_index > 0:
                                    append_entries_response.success = False
                                    append_entries_response.last_log_index = 0
                                else:
                                    append_entries_response.success = True

                                    dictionary_logs = msg["entries"]
                                    for l in dictionary_logs:
                                        self.log.append(LogEntry(**l))

                                    append_entries_response.last_log_index = self.log[-1].index
                                    append_entries_response.last_log_term = self.log[-1].term
                                    if msg["leader_commit"] > self.commit_index:
                                        self.commit_index = min(msg["leader_commit"], len(self.log))
                                

                        append_entries_response.term = self.current_term
                        if append_entries_response.success == True:
                            if msg["leader_commit"] > self.commit_index:
                                self.commit_index = min(msg["leader_commit"], len(self.log))


                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect(CLUSTER[msg["leader_id"]])
                    self.send_message(sock, append_entries_response)
                    sock.close()
                else:
                    append_entries_response = AppendEntriesResponse(node_id = self.id)
                    append_entries_response.success = False
                    append_entries_response.term = self.current_term
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect(CLUSTER[msg["leader_id"]])
                    self.send_message(sock, append_entries_response)
                    sock.close()

            case "AppendEntriesResponse":
                self.handle_response(msg)
            case "ClientCommand":
                self.process_command(msg, conn, addr)

    def election_loop(self):
        while True:
            if self.state == "leader":
                time.sleep(0.05)
                continue
            timeout = random.uniform(0.15, 0.3)
            heartbeat_received = self.heartbeat_event.wait(timeout)
            if heartbeat_received:
                with self.lock:
                    self.heartbeat_event.clear()
                continue
            else:

                with self.lock:
                    if self.log:
                        last_log_index = self.log[-1].index
                        last_log_term = self.log[-1].term
                    else:
                        last_log_index = 0
                        last_log_term = 0
                    self.current_term += 1
                    self.state = "candidate"
                    print(f"Kandidujem se: {self.id}")
                    self.voted_for = self.id
                    self.voted_for_me_total = 1

                    req = RequestVote(node_id = self.id, term = self.current_term, last_log_index = last_log_index, last_log_term = last_log_term)

                for i in CLUSTER:
                    try:
                        if i != self.id:
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.connect(CLUSTER[i])
                            self.send_message(sock, req)
                            sock.close()
                    except:
                        pass

    def vote(self, msg):
        with self.lock:
            my_vote = VoteResponse(node_id=self.id, term=self.current_term, response=False)
            vote_term = msg["term"]
            vote_last_log_index = msg["last_log_index"]
            vote_last_log_term = msg["last_log_term"]
            if vote_term < self.current_term:
                my_vote.response = False
            elif vote_term == self.current_term:
                if self.voted_for == None:
                    if self.log:
                        my_last_index = self.log[-1].index
                        my_last_term = self.log[-1].term

                        if (my_last_term > vote_last_log_term) or (my_last_term  == vote_last_log_term and my_last_index > vote_last_log_index):
                            my_vote.response = False
                        else:
                            my_vote.term = vote_term
                            self.state = "follower"
                            self.current_term = vote_term
                            self.voted_for = msg["node_id"]
                            my_vote.response = True
                    else:
                        self.state = "follower"
                        self.current_term = vote_term
                        self.voted_for = msg["node_id"]
                        my_vote.term = vote_term
                        my_vote.response = True   
                else:
                    my_vote.response = False
            else:
                if self.log:
                    my_last_index = self.log[-1].index
                    my_last_term = self.log[-1].term

                    if (my_last_term > vote_last_log_term) or (my_last_term  == vote_last_log_term and my_last_index > vote_last_log_index):
                        my_vote.response = False
                    else:
                        my_vote.term = vote_term
                        self.state = "follower"
                        self.current_term = vote_term
                        self.voted_for = msg["node_id"]
                        my_vote.response = True

                else:
                    self.state = "follower"
                    self.current_term = vote_term
                    self.voted_for = msg["node_id"]
                    my_vote.term = vote_term
                    my_vote.response = True             

        print(f"Moj glas za {msg["node_id"]} je {my_vote.response}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(CLUSTER[msg["node_id"]])
        self.send_message(sock, my_vote)
        sock.close()

    def heartbeat_loop(self):
        while True:
            if self.state == "leader":
                heartbeat = AppendEntries(leader_id = self.id, term = self.current_term)

                heartbeat.leader_commit = self.commit_index

                for i in CLUSTER:
                    try:
                        if i != self.id:
                            with self.lock:
                                entries_to_send = self.log[self.next_index[i] - 1:]
                                entries_to_send_dict = [asdict(entry) for entry in entries_to_send]
                                heartbeat.entries = entries_to_send_dict

                                heartbeat.prev_log_index = self.next_index[i] - 1 if self.next_index[i] - 1 >= 0 else 0
                                try:
                                    heartbeat.prev_log_term = self.log[heartbeat.prev_log_index - 1].term
                                except:
                                    heartbeat.prev_log_term = 0
                                
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.connect(CLUSTER[i])
                            self.send_message(sock, heartbeat)
                            sock.close()
                    except:
                        pass

                time.sleep(0.05)

            else:
                time.sleep(0.05)
                continue

    def send_raw(self, conn, raw_json_str):
        length = len(raw_json_str)
        length_to_send = (str(length) + " " * (128 - len(str(length)))).encode()
        conn.sendall(length_to_send)
        conn.sendall(raw_json_str.encode())

    def handle_response(self, msg):
        with self.lock:
            if self.state != "leader":
                return
            response_term = msg["term"]
            if response_term > self.current_term:
                self.current_term = response_term
                self.state = "follower"
                self.voted_for = None
                return
            elif response_term < self.current_term:
                return
            
            node_id = msg["node_id"]
            node_log_length = msg["last_log_index"]
            
            if msg["success"] == False:
                self.next_index[node_id] = node_log_length + 1
                return
            else:
                if node_log_length > 0:
                    print(f"SUCCESS od node {node_id}, last_log_index={node_log_length}")
                self.match_index[node_id] = max(node_log_length, self.match_index.get(node_id, 0))
                self.next_index[node_id] = self.match_index[node_id] + 1
                self.try_advance_commit()


    def try_advance_commit(self):
        length = len(self.log)
        for l in range(length, self.commit_index, -1):
            counter = 1
            for i in self.match_index:
                if self.match_index[i] >= l:
                    counter += 1
                
            if counter > len(CLUSTER) // 2:
                self.commit_index = l
                return

            

    def process_command(self, msg, conn, addr):
        with self.lock:
            if self.state != "leader":
                response = {"status": "redirect", "leader_id": self.leader_id}
                self.send_raw(conn, json.dumps(response))
                return

        command_type = msg["command"]

        match command_type:
            case "GET":
                with self.lock:
                    value = self.store.get(msg["key"], None)

                if value is not None:
                    response = {"status": "ok", "value": value}

                else:
                    response = {"status": "error", "value": "Key not found"}

                self.send_raw(conn, json.dumps(response))

            case "DEL":
                with self.lock:
                    log_entry = LogEntry(term = self.current_term, index = len(self.log) + 1, command = {"type": "DEL", "key": msg["key"]})
                    self.log.append(log_entry)

                print(f"Dodat log u listu za commit: {log_entry.command}")
                self.send_raw(conn, json.dumps({"response": "ok"}))

            case "SET":
                with self.lock:
                    log_entry = LogEntry(term = self.current_term, index = len(self.log) + 1, command = {"type": "SET", "key": msg["key"], "value": msg["value"]})
                    self.log.append(log_entry)

                print(f"Dodat log u listu za commit: {log_entry.command}")
                self.send_raw(conn, json.dumps({"response": "ok"}))
        


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type = int, help = "ID of the node you want to create")
    args = parser.parse_args()
    node = Node(id=args.id)
    print(f"Created node with id: {node.id}, state = {node.state}, term = {node.current_term}")
    node.start()