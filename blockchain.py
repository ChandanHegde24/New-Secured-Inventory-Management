import hashlib
import importlib
import json
import threading
from datetime import datetime
from typing import Any


BLOCK_HASH_ALGO_V1 = 'legacy_json_sorted_v1'
POW_ALGO_V1 = 'tx_payload_pow_v1'

try:
    pymongo_errors = importlib.import_module('pymongo.errors')
    MongoDuplicateKeyError = getattr(pymongo_errors, 'DuplicateKeyError')
    PyMongoError = getattr(pymongo_errors, 'PyMongoError')
except Exception:
    class PyMongoError(Exception):
        pass

    class MongoDuplicateKeyError(Exception):
        pass


class Blockchain:
    def __init__(self):
        self.chain = []
        self.difficulty = '00'
        self.block_hash_algo = BLOCK_HASH_ALGO_V1
        self.pow_algo = POW_ALGO_V1
        self._chain_lock = threading.RLock()

    def _to_timestamp(self, value: Any) -> float:
        if isinstance(value, datetime):
            return value.timestamp()
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _ensure_mongo_indexes(self, db):
        db.blockchain.create_index('block_index', unique=True)
        db.transactions.create_index([('block_index', 1), ('tx_order', 1)])

    def _normalize_transactions(self, transactions):
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
        payload = {
            'index': int(block_index),
            'previous_hash': previous_hash,
            'transactions': self._normalize_transactions(transactions),
            'nonce': int(nonce)
        }
        if timestamp is not None:
            payload['timestamp'] = float(timestamp)
        return payload

    def _pow_hash(self, previous_hash, transactions, block_index, nonce, timestamp=None):
        payload = self._pow_payload(previous_hash, transactions, block_index, nonce, timestamp)
        encoded_payload = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()
        return hashlib.sha256(encoded_payload).hexdigest()

    def _legacy_sort_transactions(self, transactions):
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
        prepared = []
        for tx in transactions:
            tx_time = round(float(tx['timestamp']), 6)
            prepared.append({
                'user': tx.get('user'),
                'action': tx.get('action'),
                'item': tx.get('item'),
                'quantity': int(tx.get('quantity', 0)),
                'timestamp': tx_time,
                'branch': tx.get('branch')
            })
        return prepared

    def _reload_chain_headers(self, db):
        blocks = db.blockchain.find(
            {},
            {
                '_id': 0,
                'block_index': 1,
                'timestamp': 1,
                'nonce': 1,
                'previous_hash': 1,
                'current_hash': 1,
                'hash_algo': 1,
                'pow_algo': 1
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
            if block.get('hash_algo'):
                header['hash_algo'] = block.get('hash_algo')
            if block.get('pow_algo'):
                header['pow_algo'] = block.get('pow_algo')
            self.chain.append(header)

    def sync_chain_headers(self, db):
        with self._chain_lock:
            self._reload_chain_headers(db)

    def _get_next_block_index(self, db, session=None):
        last_block = db.blockchain.find_one(
            {},
            {'block_index': 1},
            sort=[('block_index', -1)],
            session=session
        )
        return 1 if not last_block else int(last_block.get('block_index', 0)) + 1

    def load_chain(self, db):
        try:
            with self._chain_lock:
                self._ensure_mongo_indexes(db)
                self._reload_chain_headers(db)

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

                if not self.is_chain_valid(db):
                    raise RuntimeError('Blockchain validation failed. The ledger may be tampered with.')

                print('Blockchain successfully loaded and validated.')

        except Exception as err:
            raise RuntimeError(f'Error loading blockchain: {err}') from err

    def get_block_with_transactions(self, db, block_index):
        try:
            block = db.blockchain.find_one({'block_index': int(block_index)})
            if not block:
                return None

            block_dict = {
                'index': int(block.get('block_index', 0)),
                'timestamp': self._to_timestamp(block.get('timestamp')),
                'nonce': int(block.get('nonce', 0)),
                'previous_hash': block.get('previous_hash', ''),
                'transactions': []
            }
            if block.get('current_hash'):
                block_dict['current_hash'] = block.get('current_hash')
            if block.get('hash_algo'):
                block_dict['hash_algo'] = block.get('hash_algo')
            if block.get('pow_algo'):
                block_dict['pow_algo'] = block.get('pow_algo')

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
        except PyMongoError as err:
            print(f'Error fetching full block {block_index}: {err}')
            return None

    def get_previous_block(self, db):
        if self.chain:
            previous_block_header = self.chain[-1]
            return self.get_block_with_transactions(db, previous_block_header['index'])

        latest = db.blockchain.find_one({}, {'block_index': 1}, sort=[('block_index', -1)])
        if not latest:
            return None
        return self.get_block_with_transactions(db, int(latest.get('block_index', 0)))

    def hash(self, block):
        return self._legacy_hash(block)

    def proof_of_work(self, previous_hash, transactions, block_index):
        nonce = 0
        while True:
            hash_attempt = self._pow_hash(previous_hash, transactions, block_index, nonce)
            if hash_attempt.startswith(self.difficulty):
                return nonce
            nonce += 1

    def is_valid_proof(self, previous_hash, transactions, nonce, block_index, timestamp=None):
        deterministic_hash = self._pow_hash(previous_hash, transactions, block_index, nonce)
        if deterministic_hash.startswith(self.difficulty):
            return True

        if timestamp is not None:
            timestamp_hash = self._pow_hash(previous_hash, transactions, block_index, nonce, timestamp=timestamp)
            if timestamp_hash.startswith(self.difficulty):
                return True

            legacy_hash = self._legacy_pow_hash(previous_hash, transactions, block_index, nonce, timestamp)
            if legacy_hash.startswith(self.difficulty):
                return True

        return False

    def create_block(self, db, nonce, previous_hash, transactions, block_index, skip_pow_validation=False, session=None):
        transactions = self._normalize_transactions(transactions)
        transactions_for_storage = self._prepare_transactions_for_storage(transactions)
        block_timestamp = round(datetime.now().timestamp(), 6)

        if (not skip_pow_validation) and (not self.is_valid_proof(previous_hash, transactions, nonce, block_index)):
            raise ValueError('Invalid nonce for configured proof-of-work difficulty.')

        block_for_hash = {
            'index': int(block_index),
            'timestamp': block_timestamp,
            'nonce': int(nonce),
            'previous_hash': previous_hash,
            'transactions': transactions_for_storage
        }
        current_hash = self.hash(block_for_hash)

        db.blockchain.insert_one(
            {
                'block_index': int(block_index),
                'timestamp': block_timestamp,
                'nonce': int(nonce),
                'previous_hash': previous_hash,
                'current_hash': current_hash,
                'hash_algo': self.block_hash_algo,
                'pow_algo': self.pow_algo
            },
            session=session
        )

        tx_docs = []
        for tx_order, tx in enumerate(transactions_for_storage):
            tx_docs.append(
                {
                    'block_index': int(block_index),
                    'user': tx.get('user'),
                    'action': tx.get('action'),
                    'item': tx.get('item'),
                    'quantity': int(tx.get('quantity', 0)),
                    'timestamp': round(float(tx.get('timestamp', 0.0)), 6),
                    'branch': tx.get('branch'),
                    'tx_order': tx_order
                }
            )

        if tx_docs:
            db.transactions.insert_many(tx_docs, session=session)

        block_header = {
            'index': int(block_index),
            'timestamp': block_timestamp,
            'nonce': int(nonce),
            'previous_hash': previous_hash,
            'current_hash': current_hash,
            'hash_algo': self.block_hash_algo,
            'pow_algo': self.pow_algo
        }
        if not self.chain or self.chain[-1]['index'] < block_header['index']:
            self.chain.append(block_header)

        return {
            'index': int(block_index),
            'timestamp': block_timestamp,
            'nonce': int(nonce),
            'previous_hash': previous_hash,
            'current_hash': current_hash,
            'hash_algo': self.block_hash_algo,
            'pow_algo': self.pow_algo,
            'transactions': transactions_for_storage
        }

    def mine_and_create_block(self, db, transactions, session=None):
        if not transactions:
            raise ValueError('Cannot mine an empty transaction batch.')

        with self._chain_lock:
            self._ensure_mongo_indexes(db)

            max_retries = 5
            for _ in range(max_retries):
                self._reload_chain_headers(db)

                previous_block = self.get_previous_block(db)
                if previous_block is None:
                    raise RuntimeError('Cannot create a block without an existing genesis block.')

                previous_hash = self.hash(previous_block)
                block_index = self._get_next_block_index(db, session=session)
                nonce = self.proof_of_work(previous_hash, transactions, block_index)

                try:
                    return self.create_block(
                        db,
                        nonce=nonce,
                        previous_hash=previous_hash,
                        transactions=transactions,
                        block_index=block_index,
                        session=session
                    )
                except MongoDuplicateKeyError:
                    continue
                except PyMongoError as err:
                    if 'E11000' in str(err):
                        continue
                    raise

            raise RuntimeError('Could not persist mined block due to concurrent blockchain writes.')

    def rollback_block(self, db, block_index):
        db.transactions.delete_many({'block_index': int(block_index)})
        db.blockchain.delete_one({'block_index': int(block_index)})
        self.sync_chain_headers(db)

    def is_chain_valid(self, db):
        with self._chain_lock:
            self._reload_chain_headers(db)
            if not self.chain:
                return True

            previous_block_full = None
            legacy_linkage_validation_used = 0
            legacy_hash_validation_skipped = 0
            legacy_pow_skipped = 0

            for i, current_block_header in enumerate(self.chain):
                current_block_full = self.get_block_with_transactions(db, current_block_header['index'])
                if current_block_full is None:
                    print(f"Chain invalid: Failed to fetch full data for block {current_block_header['index']}")
                    return False

                if i == 0:
                    if current_block_full['previous_hash'] != '0':
                        print("Chain invalid: Genesis block previous_hash must be '0'.")
                        return False
                    previous_block_full = current_block_full
                    continue

                previous_block_hash_algo = previous_block_full.get('hash_algo')
                if previous_block_hash_algo == self.block_hash_algo:
                    expected_previous_hash = self.hash(previous_block_full)
                else:
                    expected_previous_hash = previous_block_full.get('current_hash') or self.hash(previous_block_full)
                    if previous_block_full.get('current_hash'):
                        legacy_linkage_validation_used += 1

                if current_block_full['previous_hash'] != expected_previous_hash:
                    print(f"Chain invalid: Hash linkage mismatch at block {current_block_full['index']}")
                    return False

                expected_current_hash = self.hash(current_block_full)
                stored_current_hash = current_block_full.get('current_hash')
                block_hash_algo = current_block_full.get('hash_algo')
                strict_hash_validation = (block_hash_algo == self.block_hash_algo)
                if strict_hash_validation:
                    if stored_current_hash != expected_current_hash:
                        print(f"Chain invalid: Stored hash mismatch at block {current_block_full['index']}")
                        return False
                elif stored_current_hash and stored_current_hash != expected_current_hash:
                    legacy_hash_validation_skipped += 1

                proof_valid = self.is_valid_proof(
                    current_block_full['previous_hash'],
                    current_block_full.get('transactions', []),
                    current_block_full['nonce'],
                    current_block_full['index'],
                    timestamp=current_block_full['timestamp']
                )

                block_pow_algo = current_block_full.get('pow_algo')
                strict_pow_validation = (block_pow_algo == self.pow_algo)
                if strict_pow_validation:
                    if not proof_valid:
                        print(f"Chain invalid: Invalid proof-of-work at block {current_block_full['index']}")
                        return False
                elif not proof_valid:
                    legacy_pow_skipped += 1

                previous_block_full = current_block_full

            if legacy_hash_validation_skipped:
                print(f"Warning: Strict hash validation skipped for {legacy_hash_validation_skipped} legacy block(s).")
            if legacy_pow_skipped:
                print(f"Warning: Strict PoW validation skipped for {legacy_pow_skipped} legacy block(s).")
            if legacy_linkage_validation_used:
                print(
                    f"Warning: Linkage validation relied on stored hash for "
                    f"{legacy_linkage_validation_used} legacy block link(s)."
                )

            return True
