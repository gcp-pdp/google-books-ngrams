# Google Books Ngrams ETL Dataflow

## Prerequisites

* linux/macos terminal 
* git
* [gcloud](https://cloud.google.com/sdk/install)

## Running

### Setup

Create GCS bucket to hold dataflow templates and temp files
```bash
PROJECT=$(gcloud config get-value project 2> /dev/null)
BUCKET=${PROJECT}-dataflow
gsutil mb gs://${BUCKET}/
```

### Local

```bash
TEMP_LOCATION="gs://${BUCKET}/temp"
NGRAM_DATASET="eng"
python -m beam \
--temp_location $TEMP_LOCATION \
--project $PROJECT \
--input-table "${PROJECT}.sandbox.${NGRAM_DATASET}_bert_preprocessed" \
--output-table "${PROJECT}:sandbox.${NGRAM_DATASET}_bert_embeddings" \
--batch-size 1000
```

### Dataflow

Build docker image
```bash
TEMPLATE_IMAGE="gcr.io/$PROJECT/bert-embeddings:0.1.0"
gcloud builds submit --tag $TEMPLATE_IMAGE --timeout 30m .
```
   
Create Dataflow flex template
```bash
TEMPLATE_PATH="gs://$BUCKET/templates/bert-embeddings-0.1.0.json"
gcloud dataflow flex-template build $TEMPLATE_PATH \
 --image "$TEMPLATE_IMAGE" \
 --sdk-language "PYTHON" \
 --metadata-file "metadata.json"
```

Run flex template
```bash
REGION="us-central1"
NGRAM_DATASET="eng"
gcloud dataflow flex-template run "${NGRAM_DATASET}-bert-embeddings-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --parameters input-table="$PROJECT.words_dev.${NGRAM_DATASET}_bert_preprocessed" \
    --parameters output-table="$PROJECT:words_dev.${NGRAM_DATASET}_bert_embeddings" \
    --parameters batch-size=1000 \
    --num-workers 10 \
    --max-workers 30 \
    --worker-machine-type c2-standard-8 \
    --region "$REGION"
```