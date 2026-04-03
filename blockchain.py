import hashlib
import importlib
import json
import threading
from datetime import datetime
from typing import Any

try:
    import mysql.connector
except Exception:
    mysql = None

try:
    pymongo_errors = importlib.import_module('pymongo.errors')
    MongoDuplicateKeyError = getattr(pymongo_errors, 'DuplicateKeyError')
    PyMongoError = getattr(pymongo_errors, 'PyMongoError')
except Exception:
    class PyMongoError(Exception):
        pass

    class MongoDuplicateKeyError(Exception):
        pass

if mysql is not None:
    MySQLError = mysql.connector.Error
else:
    class MySQLError(Exception):
        pass

class Blockchain:
    # Connections are passed in per operation, not stored in this class.
    def __init__(self):
        self.chain = []
        self.difficulty = "00"  # Simple difficulty (2 leading zeros)
        self._chain_lock = threading.RLock()
        self._chain_lock = threading.RLock()

    def _is_mongo_database(self, candidate: Any) -> bool:
        return candidate is not None and hasattr(candidate, "list_collection_names")

    def _to_timestamp(self, value: Any) -> float:
        if isinstance(value, datetime):
            return value.timestamp()
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _ensure_mongo_indexes(self, db):
        db.blockchain.create_index("block_index", unique=True)
        db.transactions.create_index([("block_index", 1), ("tx_order", 1)])

    def _normalize_transactions(self, transactions):
        """Returns a canonical transaction list for deterministic hashing."""
        normalized = []
        for tx in transactions:
            normalized.append({
                'user': tx.get('user'),
                'action': tx.get('action'),
                'item': tx.get('item'),
                'quantity': int(tx.get('quantity', 0)),
                'timestamp': round(float(tx.get('timestamp', 0.0)), 6),
                'branch': tx.get('branch')
            })

        return sorted(
            normalized,
            key=lambda tx: (
                tx['timestamp'],
                str(tx.get('user', '')),
                str(tx.get('action', '')),
                str(tx.get('item', '')),
                tx['quantity'],
                str(tx.get('branch', ''))
            )
        )

    def _pow_payload(self, previous_hash, transactions, block_index, nonce, timestamp=None):
        """Builds deterministic payload for proof-of-work hashing."""
        payload = {
            'index': int(block_index),
            'previous_hash': previous_hash,
            'transactions': self._normalize_transactions(transactions),
            'nonce': int(nonce)
        }

        # Kept only for backward-compatible validation of legacy blocks.
        if timestamp is not None:
            payload['timestamp'] = float(timestamp)

        return payload

    def _pow_hash(self, previous_hash, transactions, block_index, nonce, timestamp=None):
        payload = self._pow_payload(previous_hash, transactions, block_index, nonce, timestamp)
        encoded_payload = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded_payload).hexdigest()

    def _legacy_sort_transactions(self, transactions):
        """Sorts transactions like the original implementation (by timestamp only)."""
        legacy_transactions = []
        for tx in transactions:
            legacy_transactions.append({
                'user': tx.get('user'),
                'action': tx.get('action'),
                'item': tx.get('item'),
                'quantity': int(tx.get('quantity', 0)),
                'timestamp': float(tx.get('timestamp', 0.0)),
                'branch': tx.get('branch')
            })
        return sorted(legacy_transactions, key=lambda tx: tx['timestamp'])

    def _legacy_hash(self, block):
        """Original block-hash format used by existing ledgers."""
        block_copy = {
            'index': int(block.get('index', 0)),
            'timestamp': float(block.get('timestamp', 0.0)),
            'nonce': int(block.get('nonce', 0)),
            'previous_hash': block.get('previous_hash', ''),
            'transactions': self._legacy_sort_transactions(block.get('transactions', []))
        }
        encoded_block = json.dumps(block_copy, sort_keys=True).encode()
        return hashlib.sha256(encoded_block).hexdigest()

    def _legacy_pow_hash(self, previous_hash, transactions, block_index, nonce, timestamp):
        """Legacy PoW payload hash used by older blocks."""
        payload = {
            'index': int(block_index),
            'timestamp': float(timestamp),
            'previous_hash': previous_hash,
            'transactions': self._legacy_sort_transactions(transactions),
            'nonce': int(nonce)
        }
        encoded_payload = json.dumps(payload, sort_keys=True).encode()
        return hashlib.sha256(encoded_payload).hexdigest()

    def _prepare_transactions_for_storage(self, transactions):
        """Normalizes transactions to match DB DATETIME precision for stable hashing."""
        prepared = []
        for tx in transactions:
            tx_time = datetime.fromtimestamp(float(tx['timestamp'])).replace(microsecond=0)
            prepared.append({
                'user': tx.get('user'),
                'action': tx.get('action'),
                'item': tx.get('item'),
                'quantity': int(tx.get('quantity', 0)),
                'timestamp': tx_time.timestamp(),
                'branch': tx.get('branch')
            })
        return prepared

    def _acquire_db_lock(self, db_or_cursor, timeout_seconds=15):
        """Uses a DB advisory lock so multiple app instances cannot mine at once."""
        if self._is_mongo_database(db_or_cursor):
            # Mongo compatibility mode relies on optimistic insert retry and process lock.
            return

        db_or_cursor.execute("SELECT GET_LOCK(%s, %s);", (self._db_lock_name, timeout_seconds))
        lock_result = db_or_cursor.fetchone()
        if not lock_result or lock_result[0] != 1:
            raise RuntimeError("Could not acquire blockchain database lock.")

    def _release_db_lock(self, db_or_cursor):
        """Best-effort release for the DB advisory lock."""
        if self._is_mongo_database(db_or_cursor):
            return

        try:
            db_or_cursor.execute("SELECT RELEASE_LOCK(%s);", (self._db_lock_name,))
            db_or_cursor.fetchone()
            while db_or_cursor.nextset():
                db_or_cursor.fetchall()
        except MySQLError:
            # Ignore release failures: lock also auto-releases when connection closes.
            pass

    def _detect_schema_features(self, db_or_cursor):
        """Detects optional schema capabilities for backward compatibility."""
        if self._is_mongo_database(db_or_cursor):
            self._has_current_hash_column = True
            return

        db_or_cursor.execute("SHOW COLUMNS FROM blockchain LIKE 'current_hash';")
        self._has_current_hash_column = db_or_cursor.fetchone() is not None

    def _reload_chain_headers(self, db_or_cursor):
        blocks = db.blockchain.find(
            {},
            {
                '_id': 0,
                'block_index': 1,
                'timestamp': 1,
                'nonce': 1,
                'previous_hash': 1
            }
        ).sort('block_index', 1)

            self.chain = []
            for block in blocks:
                header = {
                    'index': int(block.get('block_index', 0)),
                    'timestamp': self._to_timestamp(block.get('timestamp')),
                    'nonce': int(block.get('nonce', 0)),
                    'previous_hash': block.get('previous_hash', '')
                }
                if block.get('current_hash'):
                    header['current_hash'] = block.get('current_hash')
                self.chain.append(header)

            self._has_current_hash_column = True
            return

        if self._has_current_hash_column:
            db_or_cursor.execute(
                "SELECT block_index, timestamp, nonce, previous_hash, current_hash "
                "FROM blockchain ORDER BY block_index;"
            )
        else:
            db_or_cursor.execute(
                "SELECT block_index, timestamp, nonce, previous_hash FROM blockchain ORDER BY block_index;"
            )
        blocks = db_or_cursor.fetchall()
        self.chain = []

        for block in blocks:
            header = {
                'index': int(block[0]),
                'timestamp': block[1].timestamp(),
                'nonce': int(block[2]),
                'previous_hash': block[3]
            }
            if self._has_current_hash_column:
                header['current_hash'] = block[4]
            self.chain.append(header)

    def sync_chain_headers(self, db_connection):
        """Public helper to refresh in-memory chain headers from DB."""

        with self._chain_lock:
            self._reload_chain_headers(db)

    def _get_next_block_index(self, db_or_cursor, session=None):
        """Gets the next block index from DB state, not in-memory length."""

        last_block = db.blockchain.find_one(
            {},
            {'block_index': 1},
            sort=[('block_index', -1)],
            session=session
        )
        return 1 if not last_block else int(last_block.get('block_index', 0)) + 1

    def load_chain(self, db_connection):
        """Loads blockchain headers and validates ledger integrity on startup."""
        if self._is_mongo_database(db_connection):
            try:
                with self._chain_lock:
                    self._ensure_mongo_indexes(db_connection)
                    self._detect_schema_features(db_connection)
                            db_connection,



                with db_connection.cursor() as cursor:
                    self._reload_chain_headers(cursor)
                        try:
                    if not self.is_chain_valid(validation_cursor):
                print("Blockchain successfully loaded and validated.")
            raise RuntimeError(f"Error loading blockchain: {err}") from err
    def get_block_with_transactions(self, db_or_cursor, block_index):
                    try:
                        with self._chain_lock:
                            self._ensure_mongo_indexes(db)
                            self._reload_chain_headers(db)
                block = db_or_cursor.blockchain.find_one({"block_index": int(block_index)})
                            if not self.chain:
                                print('No blockchain in DB, creating Genesis block...')
                                genesis_nonce = self.proof_of_work('0', [], block_index=1)
                                self.create_block(
                                    db,
                                    nonce=genesis_nonce,
                                    previous_hash='0',
                                    transactions=[],
                                    block_index=1,
                                    skip_pow_validation=True
                                )
                                self._reload_chain_headers(db)
                                print('Genesis block committed to database.')
                if not block:
                            if not self.is_chain_valid(db):
                                raise RuntimeError('Blockchain validation failed. The ledger may be tampered with.')
                    return None
                            print('Blockchain successfully loaded and validated.')

                    except Exception as err:
                        raise RuntimeError(f'Error loading blockchain: {err}') from err
                block_dict = {
                    'index': int(block.get('block_index', 0)),
                    'timestamp': self._to_timestamp(block.get('timestamp')),
                    'nonce': int(block.get('nonce', 0)),
                    'previous_hash': block.get('previous_hash', ''),
                    'transactions': []
                }
                if block.get('current_hash'):
                    block_dict['current_hash'] = block.get('current_hash')

                txs = db_or_cursor.transactions.find(
                    {"block_index": int(block_index)}
                ).sort([("tx_order", 1), ("_id", 1)])

                for tx in txs:
                    block_dict['transactions'].append({
                        'user': tx.get('user'),
                        'action': tx.get('action'),
                        'item': tx.get('item'),
                        'quantity': int(tx.get('quantity', 0)),
                        'timestamp': self._to_timestamp(tx.get('timestamp')),
                        'branch': tx.get('branch')
                    })

                return block_dict

            if self._has_current_hash_column:
                db_or_cursor.execute(
                    "SELECT block_index, timestamp, nonce, previous_hash, current_hash "
                    "FROM blockchain WHERE block_index = %s;",
                    (block_index,)
                )
            else:
                db_or_cursor.execute(
                    "SELECT block_index, timestamp, nonce, previous_hash FROM blockchain WHERE block_index = %s;",
                    (block_index,)
                )
            block = db_or_cursor.fetchone()
            if not block:
                return None

            block_dict = {
                'index': int(block[0]),
                'timestamp': block[1].timestamp(),
                'nonce': int(block[2]),
                'previous_hash': block[3],
                'transactions': []
            }
            if self._has_current_hash_column:
                block_dict['current_hash'] = block[4]

            db_or_cursor.execute(
                "SELECT user, action, item, quantity, timestamp, branch "
                "FROM transactions WHERE block_index = %s ORDER BY tx_id;",
                (block_index,)
            )
                    'action': tx[1],
            return None
        if self.chain:
            if not latest:
            return self.get_block_with_transactions(db_or_cursor, int(latest.get("block_index", 0)))
        while True:
        """
            if timestamp_hash.startswith(self.difficulty):
        """Persists a block header and its transactions to DB and updates in-memory headers."""
            block = db.blockchain.find_one({'block_index': int(block_index)})
            if not block:
                return None
        if (not skip_pow_validation) and (not self.is_valid_proof(previous_hash, transactions, nonce, block_index)):
            block_dict = {
                'index': int(block.get('block_index', 0)),
                'timestamp': self._to_timestamp(block.get('timestamp')),
                'nonce': int(block.get('nonce', 0)),
                'previous_hash': block.get('previous_hash', ''),
                'transactions': []
            }
            if block.get('current_hash'):
                block_dict['current_hash'] = block.get('current_hash')
            raise ValueError("Invalid nonce for configured proof-of-work difficulty.")
            txs = db.transactions.find({'block_index': int(block_index)}).sort([('tx_order', 1), ('_id', 1)])
            for tx in txs:
                block_dict['transactions'].append({
                    'user': tx.get('user'),
                    'action': tx.get('action'),
                    'item': tx.get('item'),
                    'quantity': int(tx.get('quantity', 0)),
                    'timestamp': self._to_timestamp(tx.get('timestamp')),
                    'branch': tx.get('branch')
                })

            return block_dict
        block_for_hash = {
            'index': int(block_index),
            'timestamp': timestamp_obj.timestamp(),
            'nonce': int(nonce),
            'previous_hash': previous_hash,
            'transactions': transactions_for_storage
        }
        current_hash = self.hash(block_for_hash)

        if self._is_mongo_database(db_or_cursor):
            block_doc = {
                "block_index": int(block_index),
                "timestamp": timestamp_obj,
                "nonce": int(nonce),
                "previous_hash": previous_hash,
                "current_hash": current_hash
            }
            db_or_cursor.blockchain.insert_one(block_doc, session=session)

            tx_docs = []
            for tx_order, tx in enumerate(transactions_for_storage):
                tx_docs.append({
                    "block_index": int(block_index),
                    "user": tx.get('user'),
                    "action": tx.get('action'),
                    "item": tx.get('item'),
                    "quantity": int(tx.get('quantity', 0)),
                    "timestamp": datetime.fromtimestamp(float(tx.get('timestamp', 0.0))),
                    "branch": tx.get('branch'),
                    "tx_order": tx_order
                })

            if tx_docs:
                db_or_cursor.transactions.insert_many(tx_docs, session=session)

            block_header = {
                'index': int(block_index),
                'timestamp': timestamp_obj.timestamp(),
                'nonce': int(nonce),
                'previous_hash': previous_hash,
                'current_hash': current_hash
            }

            if not self.chain or self.chain[-1]['index'] < block_header['index']:
                self.chain.append(block_header)

            return {
                'index': int(block_index),
                'timestamp': timestamp_obj.timestamp(),
                'nonce': int(nonce),
                'previous_hash': previous_hash,
                'current_hash': current_hash,
                'transactions': transactions_for_storage
            }

        if self._has_current_hash_column:
            db_or_cursor.execute(
                "INSERT INTO blockchain (block_index, timestamp, nonce, previous_hash, current_hash) "
                "VALUES (%s, %s, %s, %s, %s)",
                (int(block_index), timestamp_obj, int(nonce), previous_hash, current_hash)
            )
        else:
            db_or_cursor.execute(
                "INSERT INTO blockchain (block_index, timestamp, nonce, previous_hash) VALUES (%s, %s, %s, %s)",
                (int(block_index), timestamp_obj, int(nonce), previous_hash)
            )

        for tx in transactions_for_storage:
            tx_timestamp = datetime.fromtimestamp(float(tx['timestamp']))
            db_or_cursor.execute(
                "INSERT INTO transactions (block_index, user, action, item, quantity, timestamp, branch) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    int(block_index),
                    tx['user'],
                    tx['action'],
                    tx['item'],
                    int(tx['quantity']),
                    tx_timestamp,
                    tx['branch']
                )
            )

        block_header = {
            'index': int(block_index),
            'timestamp': timestamp_obj.timestamp(),
            'nonce': int(nonce),
            'previous_hash': previous_hash
        }
        if self._has_current_hash_column:
            block_header['current_hash'] = current_hash

        if not self.chain or self.chain[-1]['index'] < block_header['index']:
            self.chain.append(block_header)

        return {
            'index': int(block_index),
            'timestamp': timestamp_obj.timestamp(),
            'nonce': int(nonce),
            'previous_hash': previous_hash,
            'current_hash': current_hash,
            'transactions': transactions_for_storage
        }

    def mine_and_create_block(self, db_or_cursor, transactions, session=None):
        """Serializes mining + insert to keep chain order and hash links consistent."""
        if not transactions:
            raise ValueError("Cannot mine an empty transaction batch.")

        if self._is_mongo_database(db_or_cursor):
            with self._chain_lock:
                self._ensure_mongo_indexes(db_or_cursor)

                max_retries = 5
                for _ in range(max_retries):
                    self._reload_chain_headers(db_or_cursor)

                    previous_block = self.get_previous_block(db_or_cursor)
                    if previous_block is None:
                        raise RuntimeError("Cannot create a block without an existing genesis block.")

                    previous_hash = self.hash(previous_block)
                    block_index = self._get_next_block_index(db_or_cursor, session=session)
                    nonce = self.proof_of_work(previous_hash, transactions, block_index)

                    try:
                        return self.create_block(
                            db_or_cursor,
                            nonce=nonce,
                            previous_hash=previous_hash,
                            transactions=transactions,
                            block_index=block_index,
                            session=session
                        )
                    except MongoDuplicateKeyError:
                        continue
                    except PyMongoError as err:
                        if "E11000" in str(err):
                            continue
                        raise

                raise RuntimeError("Could not persist mined block due to concurrent blockchain writes.")

        with self._chain_lock:
            self._acquire_db_lock(db_or_cursor)
            try:
                # Refresh headers to avoid stale in-memory state.
                self._reload_chain_headers(db_or_cursor)

                previous_block = self.get_previous_block(db_or_cursor)
                if previous_block is None:
                    raise RuntimeError("Cannot create a block without an existing genesis block.")

                previous_hash = self.hash(previous_block)
                block_index = self._get_next_block_index(db_or_cursor)
                nonce = self.proof_of_work(previous_hash, transactions, block_index)

                return self.create_block(
                    db_or_cursor,
                    nonce=nonce,
                    previous_hash=previous_hash,
                    transactions=transactions,
                    block_index=block_index
                )
            finally:
                self._release_db_lock(db_or_cursor)

    def rollback_block(self, db_connection, block_index):
        """Best-effort rollback helper for MongoDB non-transactional fallback paths."""
        if not self._is_mongo_database(db_connection):
            return
        db_connection.transactions.delete_many({"block_index": int(block_index)})
        db_connection.blockchain.delete_one({"block_index": int(block_index)})
        self.sync_chain_headers(db_connection)

    def is_chain_valid(self, db_or_cursor):
        """Validates hash linkage and proof-of-work across the chain."""
        with self._chain_lock:
            self._detect_schema_features(db_or_cursor)
            self._reload_chain_headers(db_or_cursor)
            if not self.chain:
                return True

            previous_block_full = None
            legacy_pow_skipped = 0
            for i, current_block_header in enumerate(self.chain):
                current_block_full = self.get_block_with_transactions(db_or_cursor, current_block_header['index'])
                if current_block_full is None:
                    print(f"Chain invalid: Failed to fetch full data for block {current_block_header['index']}")
                    return False

                # Genesis block linkage check.
                if i == 0:
                    if current_block_full['previous_hash'] != '0':
                        print("Chain invalid: Genesis block previous_hash must be '0'.")
                        return False
                    previous_block_full = current_block_full
                    continue

                expected_previous_hash = self.hash(previous_block_full)
                if current_block_full['previous_hash'] != expected_previous_hash:
                    print(f"Chain invalid: Hash linkage mismatch at block {current_block_full['index']}")
                    return False

                if self._has_current_hash_column:
                    expected_current_hash = self.hash(current_block_full)
                    if current_block_full.get('current_hash') != expected_current_hash:
                        print(f"Chain invalid: Stored hash mismatch at block {current_block_full['index']}")
                        return False

                proof_valid = self.is_valid_proof(
                    current_block_full['previous_hash'],
                    current_block_full.get('transactions', []),
                    current_block_full['nonce'],
                    current_block_full['index'],
                    timestamp=current_block_full['timestamp']
                )

                if not proof_valid:
                    # Older ledgers did not persist the PoW timestamp input, so strict
                    # legacy PoW re-validation is not always possible after the fact.
                    if self._has_current_hash_column and current_block_full.get('current_hash'):
                        print(f"Chain invalid: Invalid proof-of-work at block {current_block_full['index']}")
                        return False
                    else:
                        legacy_pow_skipped += 1

                previous_block_full = current_block_full

            if legacy_pow_skipped:
                print(f"Warning: Strict PoW validation skipped for {legacy_pow_skipped} legacy block(s).")

            return True