Please replace all placeholder values in .env.example


BloodBridge — Docker-Based Setup Guide

This project is fully containerized using Docker and Docker Compose.
No local Python or PostgreSQL installation is required.

The setup steps below work identically on Windows and macOS.


Prerequisites

1. Install Docker Desktop
   Windows / macOS: https://www.docker.com/products/docker-desktop/

2. Ensure Docker is running

3. Open a terminal in the project root directory
   (the folder that contains docker-compose.yml)

4. Verify Docker installation:
   docker --version
   docker compose version


Before You Run (Important)

• The database dump file must be present in the project root directory:
  bloodbridge_full.dump

• The file init.sql is included:



Run the Project (Follow These 5 Steps Exactly)

All commands below must be run from the project root directory.



Step 1: Start the Database Container

docker compose up -d db

This starts the PostgreSQL + TimescaleDB container.



Step 2: Enable TimescaleDB Extension

docker exec -it bloodbridge-db psql -U postgres -d bloodbridge -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"

This ensures TimescaleDB is available before restoring the database.



Step 3: Restore the Database Dump

docker exec -it bloodbridge-db pg_restore -U postgres -d bloodbridge /backup/bloodbridge_full.dump

The database dump is mounted inside the container at /backup.
Schema, tables, data, and constraints are restored here.



Step 4: Verify Database Restore (Optional Check)

docker exec -it bloodbridge-db psql -U postgres -d bloodbridge -c "SELECT COUNT(*) FROM ops.inventory;"

This confirms that inventory data was restored correctly.



Step 5: Build and Start the Web Application

docker compose up --build web

This builds the Flask application container and starts the web service.


Accessing the Application

Once Step 5 completes, open a browser and visit:
http://localhost:5000


Stopping the Application

To stop all containers:
docker compose down




Clean Reset (If Needed)

If a clean database reset is required:
docker compose down -v
docker compose up -d db


Notes on Reproducibility

• Works on Windows, macOS, and Linux
• No OS-specific commands are required
• No local PostgreSQL or Python installation needed
• All dependencies run inside Docker containers
