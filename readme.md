# ArangoDB-BG

[ArangoDB](https://www.arangodb.com/) implementation for [BG-Benchmark](http://bgbenchmark.org/).

1. Copy src and lib into BG/db/ArangoDB/
2. Set Maven for arangodb-java-driver or use libraries under lib

# Usage:

Create db "bgtest" in ArangoDB.

Create schema:

`onetime -schema -db arangoDB.ArangoDbClient -p arangodb.url=127.0.0.1:8529 -p arangodb.database=bgtest`

Load data:

`onetime -load -db arangoDB.ArangoDbClient -P <HOME>/BG/workloads/populateDB -p arangodb.url=127.0.0.1:8529 -p arangodb.database=bgtest`