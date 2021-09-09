# Preparing Annoy indexes

See: https://github.com/spotify/annoy

## Environment setup
```
python3 -m venv annoy
source annoy/bin/active
pip install wheel annoy
```

## Export from BQ, prepare for Annoy

- These steps assume text was preprocessed as discussed in [#6](https://github.com/gcp-pdp/google-books-ngrams/issues/6)
- Create a two-column table of `term` and `features` (from e.g. `eng_1_bert`)
- Export the table to GCS in JSONL format
- Copy the files to local disk
- Add an 'id' column. Example output record: `{'id':1,'term':'foo','features':[0.1,0.5,-0.5,...]}`
  - preserving the `id : term` mapping: `zcat eng_1.jsonl.gz | python3 ./annoy-term2id.py > eng_1.map`

## Prep
- build the annoy index: `zcat eng_1.jsonl.gz | python3 ./annoy-index.py eng_1.ann`
  - This takes about 170GB of memory for 40M records and Annoy settings of 100 trees and Euclidean distance. Seems to scale linearly.
- split the input (ID-space) based on number of CPU cores to be used:
  ```
  mkdir terms
  pushd terms
  cat ../eng_1.map | awk {'print $1}' | split -l 500000
  popd
  ```

## Find neighbors
- find each term's nearest neighbor with the annoy index:
  ```(for i in `find terms/ -type f`; do echo "cat $i | python3 ./annoy-retrieve.py eng_1.ann | gzip > $i.jsonl.gz"; done) | parallel```
- move from ID-space to term-space:
  `zcat terms/xaa.jsonl.gz | head | python3 ./annoy-id2term.py eng_1.map`
