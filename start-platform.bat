@echo off
echo Starting Airbyte...
call abctl local start

echo Starting Postgres & MinIO...
call docker-compose up -d

echo Platform is up and running!
pause