# bigtable-autoscaler

The (amazing) Bigtable autoscaler is a backend service that periodically sends
resize commands to Bigtable clusters. It is backed by a PostgreSQL database for
keeping its state, like for example:

* current number of nodes
* number of nodes boundaries
* target CPU utilization
* last resize event

The autoscaler checks the database every 30 seconds and decides if it should 
do something or not (there are time thresholds to not resize clusters too often). 
In case it's time to check a cluster, it fetches the current CPU utilization 
from the Bigtable API. If that is different from the target CPU utilization 
(also here there are thresholds) it calculates the adequate number of nodes 
and then it sends a resize request.

The autoscaler also provides an HTTP API to insert, update and delete Bigtable 
clusters from being autoscaled.

# Development setup

This service needs to be run with a service account that has rights to
get and update cluster size.

The database schema is defined in schema.sql in this project.
