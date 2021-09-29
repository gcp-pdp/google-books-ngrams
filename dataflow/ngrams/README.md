# Google Books Ngrams ETL Dataflow

## Prerequisites

* linux/macos terminal 
* git
* [gcloud](https://cloud.google.com/sdk/install)

## Running

### Local

```bash
python -m ngrams --input-file $GCS_PATH \
--temp_location $TEMP_LOCATION \
--project $PROJECT \
--output-table "$PROJECT:$DATASET.$TABLE"
```

### Dataflow

Build docker image
```bash
TEMPLATE_IMAGE="gcr.io/$PROJECT/ngrams-beam:1.1.1"
gcloud builds submit --tag $TEMPLATE_IMAGE .
```
   
Create Dataflow flex template
```bash
TEMPLATE_PATH="gs://$BUCKET/templates/ngrams-1.1.1.json"
gcloud dataflow flex-template build $TEMPLATE_PATH \
 --image "$TEMPLATE_IMAGE" \
 --sdk-language "PYTHON" \
 --metadata-file "metadata.json"
```

Run flex template
```bash
REGION="us-central1"
gcloud dataflow flex-template run "ngram-beam-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --parameters input-file="$GCS_PATH" \
    --parameters output-table="$PROJECT:$DATASET.$TABLE" \
    --region "$REGION"
```