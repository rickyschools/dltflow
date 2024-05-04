# bin/sh

dltflow deploy-py-dlt \
  --deployment-file ../workflows/streaming_append_changes_wrkflow.yml \
  --environment dev --as-individual
