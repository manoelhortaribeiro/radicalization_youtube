import networkx as nx
import json


edges = []

with open("./recommended_20190416Apr1555420335.jsonl") as f:
    for line in f.readlines():
        edges.append(json.loads(line))

G = nx.DiGraph()

for edge in edges:
    G.add_node(edge["channel_id"], name=edge["name"],
               videoCount=int(edge["statistics"]["videoCount"]),
               viewCount=int(edge["statistics"]["viewCount"]),
               subscriberCount=int(edge["statistics"]["subscriberCount"]),
               )

for edge in edges:
    for actual_edge in edge["edges"]:
        if G.has_node(actual_edge["channel_id"]):
            G.add_edge(edge["channel_id"], actual_edge["channel_id"],
                       kind=actual_edge["type"] if actual_edge["type"] == "Canais relacionados" else "Featured channels")
pr = nx.pagerank(G)

for node in G.nodes():
    G.nodes[node]["pagerank"] = pr[node]

nx.write_graphml(G, "./recommended_20190416Apr1555420335.graphml")
