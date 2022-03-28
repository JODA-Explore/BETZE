#!/bin/bash


if [ "$#" -lt 3 ]; then
    echo "Too few arguments."
    echo "Usage: 'benchmark_queries.sh <dataset dir> <query dir> <timeout> [<docker run options>]'"
    exit 1
fi
dataset_dir="$1" 
query_dir="$2" 
TIMEOUT="$3"


JQ_IMAGE="nicotin/jq:05-10-2021"
JODA_IMAGE="ghcr.io/joda-explore/joda/joda:0.13.1"
MONGO_IMAGE="mongo:5.0.3-focal"
PSQL_IMAGE="postgres:13.4-alpine"

GLOBAL_DOCKER_OPTIONS=""

JQ_DOCKER_OPTIONS="$GLOBAL_DOCKER_OPTIONS"
MONGO_DOCKER_OPTIONS="$GLOBAL_DOCKER_OPTIONS"
JODA_DOCKER_OPTIONS="$GLOBAL_DOCKER_OPTIONS"
PSQL_DOCKER_OPTIONS="$GLOBAL_DOCKER_OPTIONS"



if ! [ -d "$query_dir" ]; then
  echo "Query directory '$query_dir' does not exist." 
  exit 1
fi

if ! [ -d "$dataset_dir" ]; then
  echo "Dataset directory '$dataset_dir' does not exist." 
  exit 1
fi


calculate_time () {
  START_STR=$(docker inspect --format='{{.State.StartedAt}}' $1)
  STOP_STR=$(docker inspect --format='{{.State.FinishedAt}}' $1)

  START=$(date --date "$START_STR" +%s%N)
  END=$(date --date "$STOP_STR" +%s%N)
  DIFF=$((END-START))
  SECS=$(awk -v diff=$DIFF "BEGIN{print diff/1000000000}")
  echo "$SECS"
} 


timeout_container() {
  # Wait for the container to end or until timeout
  code=$(timeout "$TIMEOUT" docker wait "$1" || true)
  # Kill the container. If timeout reached, container is running and will be killed, else nothing happens
  docker kill $1 &> /dev/null
  # Output status of container
  echo -n 'status: '
  if [ -z "$code" ]; then
    echo timeout
  else
    echo exited: $code
  fi
}



### JODA
joda_file="$query_dir/queries.joda"
tmp_joda_file="$query_dir/tmp.joda"
if test -f "$joda_file"; then
    echo "=== Found JODA query file, executing JODA queries ==="
    # Create JODA query file
    touch "$tmp_joda_file"
    for f in "$dataset_dir"/*.json; do
      [ -f "$f" ] || break
      # For every dataset
      file=$(basename "$f")
      echo "= File:'$file' ="
      dataset=$(basename "$f" .json)
      # Add dataset load statement to query file
      echo "LOAD $dataset FROM FILE \"/dataset/$file\" LINESEPARATED" >> "$tmp_joda_file"
    done
    # Write queries to temporary JODA file
    cat "$joda_file" >> "$tmp_joda_file"
    # Start JODA container with temporary query file
    cont=$(docker run --name=joda-benchmark --pull always -d $JODA_DOCKER_OPTIONS -v "$dataset_dir:/dataset" -v "$query_dir:/data" "$JODA_IMAGE" -c --logtostderr -f "/data/tmp.joda")

    # Wait for JODA to finish (or timeout)
    timeout_container "$cont"

    # Calculate the running time
    JODA_TIME=$(calculate_time joda-benchmark)
    echo "JODA: $JODA_TIME s"

    # Cleanup temporary files
    rm "$tmp_joda_file"

    # Store log
    docker logs joda-benchmark &> $query_dir/joda.log
    # Extract/Calculate query execution times
    JODA_EXEC_TIMES=$(cat $query_dir/joda.log | grep "\"Query\": [0-9]" | grep -o "[0-9]*\.[0-9]*" | tail -n +2)
    JODA_EXEC_TIME=$(echo "$JODA_EXEC_TIMES" | awk '{s+=$1} END {print s}')
    # Cleanup container
    docker rm "$cont"
fi

### MongoDB
# Benchmark Query file
mongo_file="$query_dir/mongo.js"
# Temporary query file
tmp_mongo_file="$query_dir/tmp.js"
# MongoDB init script
start_mongo_file="$query_dir/start-mongo.sh"
# MongoDB query script
query_mongo_file="$query_dir/query-mongo.sh"
if test -f "$mongo_file"; then
    echo "=== Found MongoDB query file, executing MongoDB queries ==="
    # Init scripts
    echo "#!/bin/bash" > "$start_mongo_file"
    echo "#!/bin/bash" > "$query_mongo_file"
    # Init Temporary query file
    echo "use benchmark;" > $tmp_mongo_file
    echo "db.setProfilingLevel(2,-1);" >> $tmp_mongo_file
    for f in "$dataset_dir"/*.json; do
      [ -f "$f" ] || break
      file=$(basename "$f")
      echo "= File:'$file' ="
      dataset=$(basename "$f" .json)
      # Add dataset load to start script
      echo "mongoimport --db benchmark --collection $dataset --file /dataset/$file || exit 1"  >> "$start_mongo_file"
    done
    cat "$mongo_file" >> "$tmp_mongo_file"
    # Add query code to query script
    echo "(mongo < /queries/tmp.js) > /queries/mongo.log | exit 1" >> "$query_mongo_file"
    # Add shutdown command after queries
    echo "mongod --shutdown" >> "$query_mongo_file"
    # Start MongoDB
    cont=$(docker run --name=mongo-benchmark --pull always -d $MONGO_DOCKER_OPTIONS -v "$dataset_dir:/dataset" -v "$query_dir:/queries" "$MONGO_IMAGE")
    # Execute initial script (Data import)
    docker exec mongo-benchmark /bin/sh -c 'bash /queries/start-mongo.sh'
    # Execute queries (detached)
    docker exec -d mongo-benchmark /bin/sh -c 'bash /queries/query-mongo.sh'
    # Wait for MongoDB to finish (or timeout)
    timeout_container "$cont"

    # Calculate the running time
    MONGO_TIME=$(calculate_time mongo-benchmark)
    echo "MongoDB: $MONGO_TIME s"
    
    # Cleanup files
    rm -f "$start_mongo_file"
    rm -f "$query_mongo_file"
    rm -f "$tmp_mongo_file"
    rm -f "$query_dir/mongo.log"

    # Store logs
    docker logs "$cont" &> $query_dir/mongo.log
    # Calculate exec times
    MONGO_EXEC_TIMES=$(cat $query_dir/mongo.log | grep "\"appName\":\"MongoDB Shell\".*\"query\".*\"durationMillis\":[0-9]*" | grep -oP "\"durationMillis\":\K[0-9]*" | awk '{ print $1/1000 }') 
    MONGO_EXEC_TIME=$(echo "$MONGO_EXEC_TIMES" | awk '{s+=$1} END {print s}')
    # Cleanup container
    docker rm "$cont"
fi


### Postgres
psql_file="$query_dir/postgres.sql"
# PSQL Init file
tmp_psql_file="$query_dir/init.sql"
# PSQL query file
query_psql_file="$query_dir/tmp_query.sql"
if test -f "$psql_file"; then
    echo "=== Found Postgres query file, executing Postgres queries ==="
    touch "$tmp_psql_file"
    for f in "$dataset_dir"/*.json; do
      [ -f "$f" ] || break
      file=$(basename "$f")
      echo "= File:'$file' ="
      dataset=$(basename "$f" .json)
      echo "CREATE UNLOGGED TABLE $dataset (doc jsonb);" >> "$tmp_psql_file"
      # Import datasets
      echo "COPY $dataset (doc) from program 'sed -e ''s/\\\\/\\\\\\\\/g'' /dataset/$file';"  >> "$tmp_psql_file"
    done
    # Add queries to query file
    cat "$psql_file" > "$query_psql_file"
    # Add shutdown to query file
    echo "COPY (SELECT 1) TO PROGRAM 'pg_ctl -D /var/lib/postgresql/data stop';" >> "$query_psql_file"
    # Start PSQL server
    cont=$(docker run --name=psql-benchmark -e POSTGRES_PASSWORD=postgres -e POSTGRES_HOST_AUTH_METHOD=trust --pull always -d $PSQL_DOCKER_OPTIONS -v "$dataset_dir:/dataset" -v "$query_dir:/data"  "$PSQL_IMAGE"  -c log_statement=all -c log_duration=on)
    # Wait for startup
    sleep 10
    # Execute initial script (Data import)
    docker exec "$cont" psql -U postgres -f /data/init.sql
    # Execute queries (detached)
    docker exec -d "$cont" psql -U postgres -f /data/tmp_query.sql

    # Wait for PSQL to finish (or timeout)
    timeout_container "$cont"

    # Calculate exec times
    PSQL_TIME=$(calculate_time psql-benchmark)
    echo "PSQL: $PSQL_TIME s"
   
    # Cleanup files
    rm "$tmp_psql_file"
    rm "$query_psql_file"

    # Calculate number of queries by counting lines in query file (excluding first comment line)
    PSQL_NUM_QUERIES=$(tail -n +2 $query_dir/postgres.sql | wc -l)
    # Calculate exec times
    PSQL_EXEC_TIMES=$(cat $query_dir/psql.log | grep "duration:" | tail -n "$PSQL_NUM_QUERIES" | grep -oP "duration: \K[0-9]*" | awk '{ print $1/1000 }') 
    PSQL_EXEC_TIME=$(echo "$PSQL_EXEC_TIMES" | awk '{s+=$1} END {print s}')
    
    # Store logs
    docker logs "$cont" &> $query_dir/psql.log
    # Cleanup container
    docker rm "$cont"
fi

### JQ
jq_file="$query_dir/jq.sh"
if test -f "$jq_file"; then
    echo "=== Found JQ query file, executing JQ queries ==="
    # Build image
    docker build -t "$JQ_IMAGE" -f benchmarking/docker/jq/Dockerfile .
    # Start JQ run
    cont=$(docker run --name=jq-benchmark $JQ_DOCKER_OPTIONS -v "$dataset_dir:/dataset" -v "$query_dir:/data" -d  "$JQ_IMAGE" /bin/sh -c 'cd /dataset; bash /data/jq.sh')
    # Wait for container to finish (or timeout)
    timeout_container "$cont"
    # Calculate exec times
    JQ_TIME=$(calculate_time jq-benchmark)
    echo "JQ: $JQ_TIME s"
    # Store logs
    docker logs "$cont" &> $query_dir/jq.log
    # Cleanup container
    docker rm "$cont"
fi

echo "=== Results ==="
echo "= JODA ="
echo "Overall: $JODA_TIME"
echo "Query Time: $JODA_EXEC_TIME"
echo "$JODA_EXEC_TIMES"
echo "= Mongo ="
echo "Overall: $MONGO_TIME"
echo "Query Time: $MONGO_EXEC_TIME"
echo "$MONGO_EXEC_TIMES"
echo "= PostgreSQL ="
echo "Overall: $PSQL_TIME"
echo "Query Time: $PSQL_EXEC_TIME"
echo "$PSQL_EXEC_TIMES"
echo "= JQ ="
echo "Overall: $JQ_TIME"

