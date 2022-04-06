# BETZE - Random Explorer Benchmark Generator
This tool enables you to generate benchmark queries on semi-structured data for multiple systems.
To generate the benchmarks, the system uses a random-explorer model, which simulates a user exploring the dataset with varying knowledge.
To get started, you only need a JSON dataset.

Currently the following systems are supported to be benchmarked:
 - [JODA](https://github.com/JODA-Explore/JODA) - a JSON data wrangling tool 
 - [PostgreSQL](https://www.postgresql.org/) - a relational database with JSON capabilities
 - [MongoDB](https://www.mongodb.com/) - a document database
 - [JQ](https://stedolan.github.io/jq/) - a CLI tool for handling JSON files


## Usage
This project can be used in a variety of ways to make it adaptable to any use case.

### Go Library 
As this project is written in Go, it can be used as a golang library in your projects.
To do so, simply add it as a module to your `go.mod` file.

### CLI
This repository also contains a ready-to-use CLI tool to analyze data and generate benchmarks.
To get more information about the usage of the CLI, execute the `cmd/betze` with the `-h` flag.
But generally creating a benchmark is a two step process.

First the dataset has to be fetched and analyzed using the following command:
```
betze fetch-dataset [command options] <Analytics provider {JODA}> [<sources ...>]
```
This will fetch an analyzed dataset from the analytics provider and store it in a `datasets.json` file.
The filename and location can be changed with the `--file` option.
Currently, only the `JODA` provider is supported. 
It will fetch any dataset currently imported into a running JODA instance.
If you have a JODA server running with imported datasets you can create the analytics file with:
```
betze fetch-dataset --joda-host "http://localhost:5632" JODA
```

After you analyzed your dataset you can generate a benchmark session with the following command:
```
betze generate [command options] <datasets.json>
```

This will generate a benchmark session with the default settings based on the analytics data in the `datasets.json` file.
Many settings are available to be changed.
But the most important ones are:
 - `--seed`: The seed for the random-explorer model. Running the generator multiple times with the same seed and dataset will result in the same queries.
 - `--preset`: A preset configuration. Currently `novice`, `intermediate`, and `expert` are supported.
 - `--joda-host`: Providing a JODA instance will enable the generator to double check the selectivities of the generated queries. This is highly recommended. The JODA instance needs the dataset to be imported.

The following command will generate an expert user session and translate the queries to MongoDB commands and store them in the `mongo.js` file:
```
betze generate --joda-host "http://localhost:5632" --preset expert --mongo-file "mongo.js" datasets.json
```

### Docker
The generator is also available as a [Docker](https://www.docker.com/) container.
The [image](https://github.com/JODA-Explore/BETZE/pkgs/container/betze%2Fbetze) is available in our GitHub repository.
To get started, simply run:

```bash
docker run --rm ghcr.io/joda-explore/betze/betze:latest
```

#### Utility scripts
To further improve the usability of the program, we also provide a few utility scripts.
These scripts use Docker to run the generator with all the required dependencies without installing them.
You can create benchmark queries with a single script invocation without installing or analyzing anything if you have Docker installed.

The `generate_queries.sh` script only takes a directory containing one line-separated JSON file per dataset and analyzes them using a [JODA docker container](https://github.com/JODA-Explore/JODA/pkgs/container/JODA%2Fjoda). 
It then translates the queries into all supported system and stores these files in the query directory.
```bash
./generate_queries.sh <dataset dir> <query dir> [<seed>] [<options>] 
```

If you, for example, have a `NoBench.json` in your `/data` directory and want to create a benchmark session with the novice preset you can call: 
```bash
./generate_queries.sh /data ~/queries 1 --preset novice
```
The command will then build and pull all required docker images, analyze the set and store the queries in the `~/queries` directory.

To benchmark this query session with all supported systems you then only have to run the `benchmark_queries.sh` script:
```bash
 ./benchmark_queries.sh <dataset dir> <query dir> [<docker run options>]
```
This script will build or pull docker images of all supported systems and execute the queries in them.
It will store the logs of each execution in the query directory and perform a simple analysis of the logs and return the runtime for each system.
All further arguments that are passed to the system will be passed to all `docker run` invocations.

To continue our example, the following command will execute our previously generated queries.
```bash
 ./benchmark_queries.sh /data ~/queries
```

### JODA Web
We also included the generator in the JODA web interface.
For ease of use, this interface is available as a [docker image](https://github.com/orgs/JODA-Explore/packages/container/package/joda-web).


# Citation
If you want to cite this project in your research, please use our ICDE 2022 paper.

## Bibtex:

```
@inproceedings{betzeicde2022,
  author    = {Nico Sch{\"{a}}fer and
               Sebastian Michel},
  title     = {BETZE: Benchmarking Data Exploration Tools with (Almost) Zero Effort},
  booktitle = {38th {IEEE} International Conference on Data Engineering, {ICDE} 2022,
               (Virtual) Kuala Lumpur, Malaysia, May 9-12, 2022},
  year      = {2022}
}
```
