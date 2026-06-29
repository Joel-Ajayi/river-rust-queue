# Migration runner: the golang-migrate CLI with our SQL baked in.
# Build context is the migrations/ dir.
FROM migrate/migrate:v4.18.1
COPY global /migrations/global
COPY shard  /migrations/shard
ENTRYPOINT []
