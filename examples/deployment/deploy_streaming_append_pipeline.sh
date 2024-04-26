# bin/sh

dltflow deploy-py-dlt \
  --deployment-file ../workflows/streaming_dlt_pipeline.yml \
  --environment dev --as-individual
