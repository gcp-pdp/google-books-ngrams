import sys
import json
from annoy import AnnoyIndex
f = 128
t = AnnoyIndex(f, 'euclidean')
i = 0
for line in sys.stdin:
    doc = json.loads(line)
    print(str(i) + "\t" + doc['term'])
    i = i + 1
