import sys
import json
term_file = sys.argv[1]

id2term = {}
tf = open(term_file, "r")
for line in tf.readlines():
    line = line.rstrip()
    F = line.split("\t")
    id2term[str(F[0])] = str(F[1])
for line in sys.stdin:
    jj = json.loads(line)
    oo = {}
    oo['term'] = id2term[str(jj['term_id'])]
    oo['hits'] = list(map(lambda x: {'term':id2term[str(jj['hit_id'][x])],'distance':jj['hit_score'][x]}, range(10)))
    print(json.dumps(oo))
