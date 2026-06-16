# Run the DDL script first
docker run --network=host -v "$(pwd)":/scripts -it --rm postgres:14.23-trixie \
  psql -h localhost -U admin -d mydatabase -f /scripts/01_create_table.sql

# Then run the DML script
docker run --network=host -v "$(pwd)":/scripts -it --rm postgres:14.23-trixie \
  psql -h localhost -U admin -d mydatabase -f /scripts/02_insert_data.sql


# docker run --network=host -it --rm postgres:14.23-trixie psql -h localhost -U admin -d mydatabase
