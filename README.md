# 🛡️ Secured Multi-Branch Inventory System with Blockchain Ledger

A robust, high-performance inventory management application built with Python (Tkinter) and a pluggable database backend (MySQL or MongoDB). This release features a secure, immutable blockchain audit log for all transactions, Role-Based Access Control (RBAC), built-in connection pooling/client management, a customized logging architecture, and a fully multi-threaded, non-blocking UI.

## 📑 Table of Contents

- [Application Dashboard](#-application-dashboard)
- [Key Features](#-key-features)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Setup & Installation](#️-setup--installation)
- [User Guide](#-user-guide)
- [License](#-license)

-----

## 🖥️ Application Dashboard

**Login Page:**

![Screenshot (31)](https://github.com/user-attachments/assets/7628eb67-1623-476c-9edb-6a468bba23f2)

-----
**Inventory Home:**

![Screenshot (29)](https://github.com/user-attachments/assets/2b1b13ee-bd46-41f8-8384-01aecec596f3)

-----
**Blockchain Ledger:**

![Screenshot (30)](https://github.com/user-attachments/assets/db99580e-3fff-49e2-bcbf-fee8fb17c5f2)

-----

## 🚀 Key Features

* **⛓️ Immutable Blockchain Ledger:** All inventory changes (adds, transfers, deletes) are recorded as transactions in a tamper-detectable blockchain using Proof-of-Work.
* **👤 Role-Based Access Control (RBAC):** Secure user (`user`) and administrator (`admin`) roles. Admins have exclusive access to view the global blockchain ledger.
* **⚡ High-Performance Architecture:**
    * **Backend Compatible:** Supports both `mysql-connector-python` and `pymongo` with environment-based backend switching.
  * **Multi-threading:** Non-blocking UI ensures a smooth, responsive user experience while executing background database workloads.
* **📈 Scalable By Design:** The blockchain uses header-only loading, meaning the app starts instantly and uses minimal RAM, even with millions of transactions.
* **🔐 Secure Credentials:** All user PINs are hashed using **bcrypt**, the industry standard for secure password hashing.
* **📦 Atomic Transactions:** Uses DB-native transactional patterns where available (MySQL transactions and MongoDB session transactions/fallback mode).
* **🏪 Multi-Branch Support:** Manage inventory and conduct seamless stock transfers seamlessly between multiple branches.
* **📝 Advanced Logging:** Standardized comprehensive Python logging to capture errors, tracebacks, and blockchain mining successes securely.

-----

## 💻 Tech Stack

* **Core:** Python 3 (incorporating Type Hints & Modular Logic)
* **GUI:** Tkinter (standard library)
* **Database:** MySQL Server or MongoDB
* **Connectors:** `mysql-connector-python`, `pymongo`
* **Security:** `bcrypt`
* **Config:** `python-dotenv`

-----

## 📁 Project Structure

```text
Secured-Inventory-Management/
├── inventory_app.py     # Main application UI and core business logic
├── blockchain.py        # Blockchain implementation (hashing, mining, validation)
├── migrate_pins.py      # Script to hash plaintext passwords to bcrypt
├── check_env.py         # Utility script to verify environment variables
├── .env                 # Environment variables for database credentials
└── README.md            # Project documentation
```

-----

## ⚙️ Setup & Installation

You can run this project using either **Docker (Recommended)** or locally.

### 🐳 Docker Deployment (Recommended)

Thanks to Docker, you can run the whole application stack (the Tkinter App + database container) seamlessly.

The provided `docker-compose.yml` defaults to MySQL, and also includes an optional MongoDB service.

1.  **Start the services from the root of the project:**
    ```bash
    docker-compose up --build -d
    ```

2.  **Displaying the GUI (X11 Forwarding):** 
    Because Tkinter requires a display interface to work, your host machine must allow X11 forwarding to stream the GUI from Docker:
    *   **Linux:** Make sure you run `xhost +local:root` on your machine before running `docker-compose up`.
    *   **Windows:** You need an X server (like VcXsrv or Xming). Set your `DISPLAY` variable in the `docker-compose.yml` to `host.docker.internal:0`.
    *   **MacOS:** You need XQuartz and have to allow connections from network clients.

3.  **Run the Database Migration Script inside the container**:
    ```bash
    docker exec -it inventory_app python migrate_pins.py
    ```

### 💻 Local Deployment

Follow these steps to get the application running locally without Docker.

#### 1\. Clone the Repository

```bash
git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name
```

### 2\. Install Dependencies

This project requires a few external Python libraries.

```bash
pip install mysql-connector-python pymongo bcrypt python-dotenv
```

### 3\. Choose a Database Backend

Set `DB_BACKEND` in your `.env` file:

* `DB_BACKEND=mysql` (default)
* `DB_BACKEND=mongodb`

### 4\. Set Up the MySQL Database (If Using MySQL)

You must have a running MySQL server.

1. Log in to your MySQL server and create the database:

    ```sql
    CREATE DATABASE inventory_db;
    ```

2. Create a dedicated user for the app (Recommended for security):

    ```sql
    -- Create a dedicated user with a strong, unique password
    CREATE USER 'inventory_app_user'@'localhost' IDENTIFIED BY 'REPLACE_WITH_A_STRONG_PASSWORD';
    GRANT SELECT, INSERT, UPDATE, DELETE ON inventory_db.* TO 'inventory_app_user'@'localhost';
    FLUSH PRIVILEGES;
    ```

    *(You can change the username and password, just make sure to update your `.env` file.)*

3. Run the following SQL in your `inventory_db` to create all necessary tables:

    ```sql
    -- 1. 'users' table (stores login info and roles)
    CREATE TABLE users (
        username VARCHAR(50) PRIMARY KEY NOT NULL,
        pin VARCHAR(60) NOT NULL, -- Increased to 60 for bcrypt
        branch VARCHAR(50) NOT NULL,
        role VARCHAR(20) NOT NULL DEFAULT 'user' -- The new RBAC column
    );

    -- 2. 'inventory' table (stores current stock)
    CREATE TABLE inventory (
        item VARCHAR(255) NOT NULL,
        quantity INT NOT NULL,
        branch VARCHAR(50) NOT NULL,
        PRIMARY KEY (item, branch) -- Composite key
    );

    -- 3. 'blockchain' table (stores block headers)
    CREATE TABLE blockchain (
        block_index INT PRIMARY KEY NOT NULL,
        timestamp DATETIME NOT NULL,
        nonce INT NOT NULL,
        previous_hash VARCHAR(64) NOT NULL,
        current_hash VARCHAR(64) NULL
    );

    -- 4. 'transactions' table (stores all transaction data)
    CREATE TABLE transactions (
        tx_id INT AUTO_INCREMENT PRIMARY KEY,
        block_index INT NOT NULL,
        user VARCHAR(50),
        action VARCHAR(50),
        item VARCHAR(255),
        quantity INT,
        timestamp DATETIME,
        branch VARCHAR(50),
        FOREIGN KEY (block_index) REFERENCES blockchain(block_index)
    );
    ```

Optional migration for existing databases:

```sql
ALTER TABLE blockchain ADD COLUMN current_hash VARCHAR(64) NULL;
```

### 5\. Create your env files

Keep database credentials in a dedicated `.env.db` file. The app loads `.env` first and then `.env.db`, so DB values in `.env.db` take priority.

Use `.env` for non-database settings if needed, and put DB connection values in `.env.db`.

```ini
# .env.db (MySQL mode)
DB_BACKEND=mysql
DB_HOST=localhost
DB_USER=inventory_app_user
DB_PASS=REPLACE_WITH_A_STRONG_PASSWORD
DB_NAME=inventory_db
```

```ini
# .env.db (MongoDB mode)
DB_BACKEND=mongodb
MONGO_URI=mongodb://localhost:27017
MONGO_DB_NAME=inventory_db
```

### 6\. Create Sample Users & Hash PINs

**This is a critical two-step process.**

1. **Insert Users with Plaintext PINs:**
    First, add your sample users to the `users` table. Use **plaintext (regular) PINs** for this one-time setup.

    ```sql
    -- Example:
    INSERT INTO users (username, pin, branch, role)
    VALUES
    ('admin1', '1234', 'Inventory_1', 'admin'),
    ('user1', '0000', 'Inventory_1', 'user'),
    ('admin2', '5678', 'Inventory_2', 'admin');
    ```

If you use MongoDB, insert equivalent documents in the `users` collection with fields `username`, `pin`, `branch`, and `role`.

2. **Run the Migration Script:**
    Now, run the `migrate_pins.py` script from your terminal. This will find all plaintext PINs, securely hash them with bcrypt, and update the database.

    ```bash
    python migrate_pins.py
    ```

### 7. Run the Application

You're all set. Launch the app:

```bash
python inventory_app.py
```

You can now log in using the credentials you created (e.g., `admin1` / `1234`).

-----

## � User Guide

Once the application is up and running, follow this guide to perform day-to-day inventory operations securely.

### 1. Logging In
* Launch the app and select your assigned **Branch** (e.g., `Inventory_1`).
* Enter your **User ID** and **PIN**. 
* The system uses Role-Based Access Control (RBAC), meaning your experience will differ based on whether you are a `user` or an `admin`.

### 2. Managing Inventory (Adding/Updating Stock)
* In the **Item Name** field, type the name of the product you want to manage.
* In the **Quantity** field, enter the number of items:
  * To add new stock: Enter a positive integer (e.g., `50`).
  * To deduct stock (e.g., sales, damaged goods): Enter a negative integer (e.g., `-5`).
* Click **Add/Update Stock**. The backend will instantly hash this transaction into a new block via Proof-of-Work and securely update your branch's inventory tab dynamically.

### 3. Inter-Branch Stock Transfers
* Click the **Stock Transfer** button to move inventory to another location (e.g., to `Inventory_2`).
* A modal window will appear. Select the **Target Branch** from the dropdown menu.
* Select the **Item** you want to transfer (only items currently in stock will be shown).
* Enter the **Quantity** (must be a positive integer) and click **Confirm Transfer**. 
* The app fully automates an atomic transfer: locking rows, verifying current stock, safely deducting from `Inventory_1`, safely depositing to `Inventory_2`, and mining a secure blockchain event.

### 4. Search and Filters
* In the top search bar labeled **Search Product**, simply begin typing characters.
* The Treeview table will instantly filter the live inventory list as you type.
* Click **Clear** to reset the view. 

### 5. Reviewing the Blockchain Ledger (Admins Only)
* If your account has the `admin` role, you will see a **View Blockchain** button.
* Click it to open a read-only terminal showing the immutable, global ledger. 
* Every block displays its **Nonce**, **Timestamp**, **Previous Hash**, and nested **Transactions** containing the Branch, User, Action, Item, and Quantities modified. 
* This provides a pure, tamper-proof audit trail for business compliance.

-----

## �📜 License

This project is licensed under the MIT License. See the `LICENSE` file for details.
