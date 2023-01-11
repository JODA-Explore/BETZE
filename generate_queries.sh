#!/bin/bash



if [ "$#" -lt 2 ]; then
    echo "Too few arguments."
    echo "Usage: 'generate_queries.sh <dataset dir> <query dir> [<seed>] [<options]'"
    exit 1
fi
dataset_dir="$1" 
query_analyze_dir="$2" 

if ! [ -d "$query_analyze_dir" ]; then
  echo "Query directory '$query_analyze_dir' does not exist." 
  exit 1
fi

if ! [ -d "$dataset_dir" ]; then
  echo "Dataset directory '$dataset_dir' does not exist." 
  exit 1
fi

if [ "$#" -ge 3 ]; then
  seed="--seed $3" # Seed used for random explorer
else
  seed=""
fi

# Build Explorer image
echo "=== (1) Build Explorer image ==="
docker build -t betze . || exit 1

# Run JODA server with dataset mounted 
echo "=== (2) Start JODA Server ==="
docker run --pull always --rm -d  -v "$dataset_dir:/dataset" -p "5632:5632" --name explorer-joda ghcr.io/joda-explore/joda/joda:0.13.1 || exit 1

# Import datasets into JODA
echo "=== (3) Import dataset(s) into JODA ==="
for f in "$dataset_dir"/*.json; do
    [ -f "$f" ] || break
    file=$(basename "$f")
    echo "= File:'$file' ="
    dataset=$(basename "$f" .json)
    docker exec explorer-joda joda-client --address localhost --port 5632 --query "LOAD $dataset FROM FILE \"/dataset/$file\" LINESEPARATED" -c 0 || exit 1
done



# Analyze dataset
echo "=== (4) Analyze dataset ==="
docker run --rm --add-host=host.docker.internal:host-gateway -v "$query_analyze_dir:/data" betze fetch-dataset --joda-host http://host.docker.internal:5632 --file /data/dataset.json JODA || exit 1

# Generate queryset
echo "=== (5) Generate queryset ==="
docker run --rm --add-host=host.docker.internal:host-gateway -v "$query_analyze_dir:/data" betze generate $seed "${@:4}" --betze-file /data/betze.json --mongo-file /data/mongo.js --joda-file /data/queries.joda --jq-file /data/jq.sh --psql-file /data/postgres.sql --spark-file /data/spark.sc --joda-host http://host.docker.internal:5632 /data/dataset.json || exit 1

# Stop JODA Server
echo "=== (6) Stop JODA Server ==="
docker stop explorer-joda