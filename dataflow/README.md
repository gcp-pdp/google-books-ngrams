# Words ETL Dataflow

## Prerequisites

* linux/macos terminal 
* git
* [gcloud](https://cloud.google.com/sdk/install)

## Setting Up

1. Build docker image
   ```bash
   TEMPLATE_IMAGE="gcr.io/$PROJECT/ngrams-beam:latest"
   gcloud builds submit --tag $TEMPLATE_IMAGE .
   ```
   
2. Create Dataflow flex template
   ```bash
   TEMPLATE_PATH="gs://$BUCKET/templates/ngrams-beam.json"
   gcloud dataflow flex-template build $TEMPLATE_PATH \
    --image "$TEMPLATE_IMAGE" \
    --sdk-language "PYTHON" \
    --metadata-file "metadata.json"
   ```
   
## Running

```bash
REGION="us-central1"
gcloud dataflow flex-template run "ngram-beam-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --parameters input-file="$GCS_PATH" \
    --parameters output-table="$PROJECT:$DATASET.$TABLE" \
    --region "$REGION"
```