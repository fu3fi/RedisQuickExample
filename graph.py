import json
from itertools import combinations
from collections import defaultdict
import redis
from prettytable import PrettyTable
from hashlib import sha256
from py2neo import Graph, Node, Relationship

def subtract_lists(arr_1, arr_2):
    arr_1 = list(map(int, arr_1))
    arr_2 = list(map(int, arr_2))
    return list(map(lambda x: x[0] - x[1], zip(arr_1, arr_2)))

with open('data/episodes_2.json', 'r') as open_file:
    data_j = json.loads(open_file.read())

graph_list = list()

for episode in data_j['episodes']:
    scene_dict = defaultdict(int)
    for scene in episode['scenes']:
        if len(scene['characters']) < 2:
            continue
        scene_time = sum(list(map(lambda x: 60**(2-x[0])*x[1], enumerate(subtract_lists(scene['sceneEnd'].split(':'), scene['sceneStart'].split(':'))))))
        for x in combinations(list(map(lambda x: x['name'], scene['characters'])), 2):
            scene_dict[tuple(sorted(x))] += scene_time
    graph_list.append((episode['episodeTitle'], scene_dict,))


graph = redis.Redis(host='localhost', port=13, db=0)

for episode_info in graph_list:
    for character_names, weight in episode_info[1].items():
        buff = dict()
        buff['character_1'] = character_names[0].replace(' ', '_').replace('\'', '')
        buff['character_2'] = character_names[1].replace(' ', '_').replace('\'', '')
        buff['episode_name'] = episode_info[0].replace(',', '_').replace(' ', '_').replace('\'', '')
        reply = graph.execute_command('GRAPH.QUERY', 
            'social', 
            f"CREATE (:Character {{name:'{buff['character_1']}'}})-[:{buff['episode_name']}]->(:Character {{name:'{buff['character_2']}'}})")
        buff = None

info = graph.execute_command('GRAPH.QUERY', 'social',
    f'''
    MATCH (n)<-[r]->(z)
    WITH n.name as name, count(r) as size 
    RETURN name, size 
    ORDER BY size DESC 
    LIMIT 10
''')

table = PrettyTable()
table.field_names = ["HeroName", "CountEdges",]
for x in info[1:-1][0]:
    table.add_row(x)
print(table)

graph.execute_command('GRAPH.QUERY', 'social',
    f'''
    MATCH (n)
    DETACH DELETE n
''')

graph_redis = redis.Redis(host='localhost', port=6379, db=5)
graph_neo4j = Graph("bolt://neo4j:neo4j!@localhost:7687")

for episode_info in graph_list:
    for character_names, weight in episode_info[1].items():
        node_1 = Node("Character", name=character_names[0].replace(' ', '_'))
        node_2 = Node("Character", name=character_names[1].replace(' ', '_'))
        relationship = Relationship(node_1, episode_info[0].replace(' ', '_'), node_2, weight=weight)
        graph_neo4j.merge(relationship, "Character", "name")


def get_cache(cursor_redis, cursor_neo4j, query, timeout):
    query_hash = sha256(query.encode()).hexdigest()
    print(query_hash)
    if not cursor_redis.exists(query_hash):
        print('not cache')
        cursor_redis.set(query_hash, json.dumps(list(cursor_neo4j.run(query))), timeout)
    return json.loads(cursor_redis.get(query_hash))

query = f'''
    MATCH (n)<-[r]->(z)
    WITH n.name as name, count(r) as size 
    RETURN name, size 
    ORDER BY size DESC 
    LIMIT 10
'''

info = get_cache(graph_redis, graph_neo4j, query, 20)

table = PrettyTable()
table.field_names = ["HeroName", "CountEdges",]
for x in info:
    table.add_row([*x])
print(table)

graph.run(f'''
    MATCH (n)
    DETACH DELETE n
''')