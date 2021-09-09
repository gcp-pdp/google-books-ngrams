import sys
import json
from annoy import AnnoyIndex
index_file = sys.argv[1]

f = 128
t = AnnoyIndex(f, 'euclidean')
i = 0
for line in sys.stdin:
    if i % 100000 == 0:
        print(str(i) + " processed")
    line = line.rstrip()
    doc = json.loads(line)
    t.add_item(doc["id"], doc["features"])
    i = i + 1
t.build(100)
t.save(index_file)
