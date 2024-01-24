from cassandra.cluster import Cluster
import sys


if __name__ == '__main__':
    match_id = sys.argv[1]
    
    cluster = Cluster(['93.175.29.116'])
    session = cluster.connect('ovchinnikova')
    
    query = "select * from ovchinnikova.top_ids_my_match where match_id = '" \
                                                    + match_id + "' limit 20"
    rows = session.execute(query)
    
    print('match_id', 'ID', 'count')
    for row in rows:
        print(row.match_id, row.ID, row.count)
        
    cluster.shutdown()
