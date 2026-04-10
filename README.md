# Secured Multi-Branch Inventory System (MongoDB)

A secure inventory management application built with Python (Streamlit) and MongoDB.
All inventory mutations are written to a blockchain-style ledger with proof-of-work.

## Features

- Role-based login (admin and user)
- Multi-branch inventory management
- Add/update stock, delete products, and inter-branch transfer
- Immutable blockchain ledger view for admins
- BCrypt PIN hashing and migration helper
- MongoDB transactions when available with safe fallback mode

## Project Files

- inventory_app.py: Streamlit web UI and business logic
- blockchain.py: MongoDB-backed blockchain implementation
- migrate_pins.py: Hashes plaintext user PINs in MongoDB
- check_env.py: Validates environment files and Mongo settings
- docker-compose.yml: MongoDB + app containers
- .env.db: Database-only environment variables

## Requirements

- Python 3.10+
- MongoDB 7+ (local or container)

Install dependencies:

```bash
pip install -r requirements.txt
```

## Environment Setup

Use a dedicated DB env file.

File: .env.db

```ini
MONGO_URI=mongodb://localhost:27017
MONGO_DB_NAME=inventory_db
```

Optional custom env filenames:

- APP_ENV_FILE (default: .env)
- DB_ENV_FILE (default: .env.db)

The app loads APP_ENV_FILE first, then DB_ENV_FILE.

## MongoDB Collections

The app uses these collections:

- users
- inventory
- blockchain
- transactions

Indexes are created automatically at startup.

## Seed Users

Insert users into the users collection with fields:

- username
- pin
- branch
- role

Example roles: admin, user

On a fresh MongoDB database, the app now auto-seeds these default users at startup:

- admin1 / 1234 (Inventory_1, admin)
- user1 / 1234 (Inventory_1, user)
- admin2 / 1234 (Inventory_2, admin)
- user2 / 1234 (Inventory_2, user)

Then run PIN migration to hash plaintext PINs:

```bash
python migrate_pins.py
```

## Run Locally

```bash
streamlit run inventory_app.py
```

Then open: <http://localhost:8501>

## Run with Docker

```bash
docker-compose up --build -d
```

Then open: <http://localhost:8501>

The app container uses:

- MONGO_URI
- MONGO_DB_NAME

## Verify Environment

```bash
python check_env.py
```
