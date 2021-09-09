import sys
import json
from annoy import AnnoyIndex
index_file = sys.argv[1]

f = 128
u = AnnoyIndex(f, 'euclidean')
u.load(index_file)
for term_id in sys.stdin:
    term_id = term_id.rstrip()
    nn = u.get_nns_by_item(int(term_id), 11, include_distances=True)
    jj = {}
    jj["term_id"] = nn[0][0]
    nn[0].pop(0)
    nn[1].pop(0)
    jj["hit_id"] = nn[0]
    jj["hit_score"] = nn[1]
    print(json.dumps(jj))
