from dataclasses import dataclass, field

"""LogEntry is a set of commands which the client has requested to be executed."""
@dataclass
class LogEntry:
    term: int
    index: int
    command: dict = field(default_factory = dict)

"""A candidate node requests to be voted as a leader. The class tracks which node is the candidate as well as the term in which the candidate is running to be the leader"""
@dataclass
class RequestVote:
    node_id: int
    term: int
    last_log_index: int
    last_log_term: int

"""The responses of each node to the candidate node running for leader"""
@dataclass
class VoteResponse:
    node_id: int
    response: bool
    term: int

"""A list of logs which need to be added to each follower node"""
@dataclass
class AppendEntries:
    leader_id: int
    term: int
    prev_log_index: int = 0
    prev_log_term: int = 0
    leader_commit: int = 0
    entries: list = field(default_factory = list)

"""The response of each follower node to the request of committing the entries"""
@dataclass
class AppendEntriesResponse:
    node_id: int
    success: bool = False
    term: int = 0
    last_log_index: int = 0