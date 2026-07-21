# Seed runner: alpine postgres image with our SQL and shell script baked in.
# Build context is the seed/ dir.
FROM postgres:16-alpine
COPY . /seeds/
RUN chmod +x /seeds/seed-performance.sh
WORKDIR /seeds
