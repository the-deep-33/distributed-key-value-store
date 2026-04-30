import os

"""Cluster of nodes which represent the servers in the store"""

def parse_cluster():
    raw = os.getenv("CLUSTER", "1=127.0.0.1:5001,2=127.0.0.1:5002,3=127.0.0.1:5003")
    clusters = {}
    for cluster in raw.split(","):
        node_id_string, addr = cluster.split("=")
        node_id = int(node_id_string)
        host, port_string = addr.split(":")
        port = int(port_string)
        clusters[node_id] = (host, port)

    return clusters

CLUSTER = parse_cluster()