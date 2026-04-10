import logging
import os
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import bcrypt
import streamlit as st
from dotenv import load_dotenv

from blockchain import Blockchain

try:
    from pymongo import MongoClient
    from pymongo.errors import ConfigurationError, InvalidOperation, OperationFailure, PyMongoError
except Exception:
    MongoClient = None

    class PyMongoError(Exception):
        pass

    class OperationFailure(Exception):
        pass

    class ConfigurationError(Exception):
        pass

    class InvalidOperation(Exception):
        pass


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

APP_ENV_FILE = os.environ.get('APP_ENV_FILE', '.env')
DB_ENV_FILE = os.environ.get('DB_ENV_FILE', '.env.db')
load_dotenv(APP_ENV_FILE)
load_dotenv(DB_ENV_FILE, override=True)

MONGO_URI = os.environ.get('MONGO_URI')
MONGO_DB_NAME = os.environ.get('MONGO_DB_NAME') or 'inventory_db'
BCRYPT_PREFIXES = ('$2a$', '$2b$', '$2y$')
ALL_BRANCHES = ['Inventory_1', 'Inventory_2']


def _is_mongo_database(candidate: Any) -> bool:
    return (
        candidate is not None
        and hasattr(candidate, 'list_collection_names')
        and hasattr(candidate, 'client')
        and hasattr(candidate, 'name')
    )


def _is_mongo_transaction_unsupported(err: Exception) -> bool:
    message = str(err).lower()
    checks = [
        'transaction numbers are only allowed',
        'does not support sessions',
        'replica set',
        'not supported'
    ]
    return any(check in message for check in checks)


def run_mongo_transaction(db: Any, operation: Callable[[Any], Any]) -> Tuple[Any, bool]:
    """
    Runs an operation inside a MongoDB transaction when available.
    Falls back to non-transactional execution on standalone servers.
    Returns: (operation_result, used_transaction)
    """
    if not _is_mongo_database(db):
        raise RuntimeError('MongoDB connection required for run_mongo_transaction.')

    try:
        with db.client.start_session() as session:
            with session.start_transaction():
                result = operation(session)
            return result, True
    except (OperationFailure, ConfigurationError, InvalidOperation, PyMongoError) as tx_err:
        if _is_mongo_transaction_unsupported(tx_err):
            logging.warning('MongoDB transactions unavailable, running without transaction: %s', tx_err)
            return operation(None), False
        raise


def _initialize_mongo_collections(db: Any):
    db.users.create_index('username', unique=True)
    db.inventory.create_index([('item', 1), ('branch', 1)], unique=True)
    db.blockchain.create_index('block_index', unique=True)
    db.transactions.create_index([('block_index', 1), ('tx_order', 1)])


def _is_bcrypt_hash(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    return len(stripped) >= 60 and any(stripped.startswith(prefix) for prefix in BCRYPT_PREFIXES)


def _verify_user_pin(input_pin: str, stored_pin: Any) -> Tuple[bool, bool]:
    """
    Returns:
    - is_valid: whether input_pin matches stored_pin
    - needs_upgrade: whether stored_pin is legacy plaintext and should be hashed
    """
    if isinstance(stored_pin, bytes):
        try:
            return bcrypt.checkpw(input_pin.encode('utf-8'), stored_pin), False
        except ValueError:
            return False, False

    stored_pin_text = str(stored_pin or '').strip()
    if not stored_pin_text:
        return False, False

    if _is_bcrypt_hash(stored_pin_text):
        try:
            return bcrypt.checkpw(input_pin.encode('utf-8'), stored_pin_text.encode('utf-8')), False
        except ValueError:
            return False, False

    is_plaintext_match = (input_pin == stored_pin_text)
    return is_plaintext_match, is_plaintext_match


def _seed_default_users_if_empty(db: Any):
    """Seeds baseline users so role-based login works on a fresh database."""
    if db.users.count_documents({}) > 0:
        return

    seed_users = [
        {'username': 'admin1', 'pin': '1234', 'branch': 'Inventory_1', 'role': 'admin'},
        {'username': 'user1', 'pin': '1234', 'branch': 'Inventory_1', 'role': 'user'},
        {'username': 'admin2', 'pin': '1234', 'branch': 'Inventory_2', 'role': 'admin'},
        {'username': 'user2', 'pin': '1234', 'branch': 'Inventory_2', 'role': 'user'},
    ]

    docs_to_insert = []
    for user_doc in seed_users:
        docs_to_insert.append(
            {
                'username': user_doc['username'],
                'pin': bcrypt.hashpw(user_doc['pin'].encode('utf-8'), bcrypt.gensalt()).decode('utf-8'),
                'branch': user_doc['branch'],
                'role': user_doc['role']
            }
        )

    db.users.insert_many(docs_to_insert, ordered=True)
    logging.info('Seeded default users for fresh database bootstrap.')


@st.cache_resource(show_spinner=False)
def initialize_resources() -> Tuple[Any, Blockchain]:
    if MongoClient is None:
        raise RuntimeError('pymongo is not installed. Install dependencies from requirements.txt.')
    if not MONGO_URI:
        raise RuntimeError('MONGO_URI is missing in environment settings.')

    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    mongo_client.admin.command('ping')
    mongo_db = mongo_client[MONGO_DB_NAME]
    _initialize_mongo_collections(mongo_db)
    _seed_default_users_if_empty(mongo_db)

    blockchain = Blockchain()
    blockchain.load_chain(mongo_db)
    return mongo_db, blockchain


def authenticate_user(db: Any, username: str, pin: str, selected_branch: str) -> Tuple[bool, str, Optional[str]]:
    if not selected_branch:
        return False, 'Please select a branch.', None
    if not username or not pin:
        return False, 'User ID and PIN cannot be empty.', None

    result_doc = db.users.find_one(
        {'username': username},
        {'_id': 0, 'pin': 1, 'branch': 1, 'role': 1}
    )

    if not result_doc:
        if db.users.count_documents({}) == 0:
            return False, 'No users found. Restart the app and try admin1 / 1234.', None
        return False, 'Invalid user ID or PIN.', None

    stored_pin = result_doc.get('pin')
    user_branch = str(result_doc.get('branch') or '').strip()
    user_role = str(result_doc.get('role') or 'user').strip().lower()

    pin_valid, pin_needs_upgrade = _verify_user_pin(pin, stored_pin)
    if user_branch != selected_branch or not pin_valid:
        return False, 'Invalid credentials or wrong branch.', None

    if pin_needs_upgrade:
        try:
            upgraded_hash = bcrypt.hashpw(pin.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            db.users.update_one({'username': username}, {'$set': {'pin': upgraded_hash}})
        except PyMongoError as upgrade_err:
            logging.warning('Could not auto-upgrade plaintext PIN for %s: %s', username, upgrade_err)

    return True, 'Login successful.', user_role


def load_inventory(db: Any, branch: str) -> Dict[str, int]:
    data = db.inventory.find(
        {'branch': branch},
        {'_id': 0, 'item': 1, 'quantity': 1}
    )
    return {
        entry.get('item'): int(entry.get('quantity', 0))
        for entry in data
        if entry.get('item')
    }


def _rollback_mined_block(db: Any, blockchain: Blockchain, block_index: Optional[int], committed: bool):
    if db is None or block_index is None or committed:
        return
    try:
        blockchain.rollback_block(db, block_index)
    except Exception:
        pass


def add_update_stock(db: Any, blockchain: Blockchain, current_user: str, current_branch: str, item: str, qty_change: int) -> Tuple[bool, str]:
    mined_block_index = None
    operation_committed = False

    try:
        preview_doc = db.inventory.find_one(
            {'item': item, 'branch': current_branch},
            {'_id': 0, 'quantity': 1}
        )
        preview_qty = int(preview_doc.get('quantity', 0)) if preview_doc else 0
        preview_new_qty = preview_qty + qty_change
        if preview_new_qty < 0:
            return False, f'Cannot remove {abs(qty_change)} units. Only {preview_qty} units of "{item}" are in stock.'

        transactions_batch = [{
            'user': current_user,
            'action': 'Add/Update',
            'item': item,
            'quantity': qty_change,
            'timestamp': time.time(),
            'branch': current_branch
        }]

        def mongo_operation(session):
            nonlocal mined_block_index

            block = blockchain.mine_and_create_block(db, transactions_batch, session=session)
            mined_block_index = block['index']

            if qty_change < 0:
                update_result = db.inventory.update_one(
                    {
                        'item': item,
                        'branch': current_branch,
                        'quantity': {'$gte': abs(qty_change)}
                    },
                    {'$inc': {'quantity': qty_change}},
                    session=session
                )
                if update_result.modified_count == 0:
                    raise ValueError(f'Stock changed during processing. Units for "{item}" are no longer sufficient.')
            else:
                db.inventory.update_one(
                    {'item': item, 'branch': current_branch},
                    {
                        '$inc': {'quantity': qty_change},
                        '$setOnInsert': {'item': item, 'branch': current_branch}
                    },
                    upsert=True,
                    session=session
                )

            current_doc = db.inventory.find_one(
                {'item': item, 'branch': current_branch},
                {'_id': 0, 'quantity': 1},
                session=session
            )
            return int(current_doc.get('quantity', 0)) if current_doc else 0

        new_qty, _ = run_mongo_transaction(db, mongo_operation)
        operation_committed = True

        if qty_change > 0:
            return True, f'Added {qty_change} units to "{item}". New total: {new_qty}.'
        return True, f'Removed {abs(qty_change)} units from "{item}". New total: {new_qty}.'

    except PyMongoError as err:
        _rollback_mined_block(db, blockchain, mined_block_index, operation_committed)
        return False, f'Database error: {err}. Transaction rolled back.'
    except Exception as err:
        _rollback_mined_block(db, blockchain, mined_block_index, operation_committed)
        return False, f'Unexpected error: {err}. Transaction rolled back.'


def delete_product(db: Any, blockchain: Blockchain, current_user: str, current_branch: str, item_name: str) -> Tuple[bool, str]:
    mined_block_index = None
    operation_committed = False

    try:
        existing = db.inventory.find_one(
            {'item': item_name, 'branch': current_branch},
            {'_id': 0, 'quantity': 1}
        )
        if not existing:
            return False, f'Product "{item_name}" no longer exists in {current_branch}.'

        transactions_batch = [{
            'user': current_user,
            'action': 'Delete Product',
            'item': item_name,
            'quantity': 0,
            'timestamp': time.time(),
            'branch': current_branch
        }]

        def mongo_operation(session):
            nonlocal mined_block_index
            block = blockchain.mine_and_create_block(db, transactions_batch, session=session)
            mined_block_index = block['index']

            delete_result = db.inventory.delete_one(
                {'item': item_name, 'branch': current_branch},
                session=session
            )
            if delete_result.deleted_count == 0:
                raise ValueError(f'Product "{item_name}" no longer exists in {current_branch}.')

        run_mongo_transaction(db, mongo_operation)
        operation_committed = True
        return True, f'Product "{item_name}" deleted successfully from {current_branch}.'

    except PyMongoError as err:
        _rollback_mined_block(db, blockchain, mined_block_index, operation_committed)
        return False, f'Failed to delete product: {err}. Transaction rolled back.'
    except Exception as err:
        _rollback_mined_block(db, blockchain, mined_block_index, operation_committed)
        return False, f'Unexpected error: {err}. Transaction rolled back.'


def execute_stock_transfer(
    db: Any,
    blockchain: Blockchain,
    current_user: str,
    current_branch: str,
    item: str,
    quantity: int,
    to_branch: str
) -> Tuple[bool, str]:
    mined_block_index = None
    operation_committed = False

    try:
        preview_doc = db.inventory.find_one(
            {'item': item, 'branch': current_branch},
            {'_id': 0, 'quantity': 1}
        )
        preview_stock_source = int(preview_doc.get('quantity', 0)) if preview_doc else 0
        if quantity > preview_stock_source:
            return False, f'Only {preview_stock_source} units are currently available in {current_branch}.'

        tx_time = time.time()
        transactions_batch = [
            {
                'user': current_user,
                'action': 'Transfer Out',
                'item': item,
                'quantity': -quantity,
                'timestamp': tx_time,
                'branch': current_branch
            },
            {
                'user': current_user,
                'action': 'Transfer In',
                'item': item,
                'quantity': quantity,
                'timestamp': tx_time + 0.001,
                'branch': to_branch
            }
        ]

        def mongo_operation(session):
            nonlocal mined_block_index
            block = blockchain.mine_and_create_block(db, transactions_batch, session=session)
            mined_block_index = block['index']

            source_result = db.inventory.update_one(
                {
                    'item': item,
                    'branch': current_branch,
                    'quantity': {'$gte': quantity}
                },
                {'$inc': {'quantity': -quantity}},
                session=session
            )
            if source_result.modified_count == 0:
                raise ValueError(f'Stock level changed. Unable to transfer {quantity} units of "{item}".')

            db.inventory.update_one(
                {'item': item, 'branch': to_branch},
                {
                    '$inc': {'quantity': quantity},
                    '$setOnInsert': {'item': item, 'branch': to_branch}
                },
                upsert=True,
                session=session
            )

        run_mongo_transaction(db, mongo_operation)
        operation_committed = True
        return True, f'Transferred {quantity} units of "{item}" from {current_branch} to {to_branch}.'

    except PyMongoError as err:
        _rollback_mined_block(db, blockchain, mined_block_index, operation_committed)
        return False, f'Transfer failed: {err}. Transaction rolled back.'
    except Exception as err:
        _rollback_mined_block(db, blockchain, mined_block_index, operation_committed)
        return False, f'Unexpected transfer error: {err}. Transaction rolled back.'


def get_blockchain_records(db: Any, blockchain: Blockchain) -> List[Dict[str, Any]]:
    blockchain.sync_chain_headers(db)
    records = []
    for block_header in blockchain.chain[:]:
        block = blockchain.get_block_with_transactions(db, block_header['index'])
        if block is not None:
            records.append(block)
    return records


def _initialize_session_state():
    defaults = {
        'authenticated': False,
        'current_user': None,
        'current_branch': None,
        'current_role': None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _reset_auth_state():
    st.session_state.authenticated = False
    st.session_state.current_user = None
    st.session_state.current_branch = None
    st.session_state.current_role = None


def render_login(db: Any):
    st.subheader('Multi-Branch Inventory Login')

    with st.container(border=True):
        with st.form('login_form'):
            selected_branch = st.selectbox('Select Branch', ALL_BRANCHES)
            user = st.text_input('User ID')
            pin = st.text_input('PIN', type='password')
            login_submitted = st.form_submit_button('Login', type='primary', use_container_width=True)

        if login_submitted:
            try:
                login_ok, login_message, user_role = authenticate_user(db, user.strip(), pin, selected_branch)
                if login_ok:
                    st.session_state.authenticated = True
                    st.session_state.current_user = user.strip()
                    st.session_state.current_branch = selected_branch
                    st.session_state.current_role = user_role
                    st.success(login_message)
                    st.rerun()
                else:
                    st.error(login_message)
            except PyMongoError as err:
                st.error(f'Database error during login: {err}')
            except Exception as err:
                st.error(f'Unexpected login error: {err}')

    st.caption('Sample login: admin1 / 1234, user1 / 1234, admin2 / 1234, user2 / 1234')


def render_inventory_dashboard(db: Any, blockchain: Blockchain):
    current_user = st.session_state.current_user
    current_branch = st.session_state.current_branch
    current_role = st.session_state.current_role

    with st.sidebar:
        st.markdown('### Session')
        st.write(f'User: {current_user}')
        st.write(f'Role: {current_role}')
        st.write(f'Branch: {current_branch}')

        if st.button('Switch Branch', use_container_width=True):
            _reset_auth_state()
            st.rerun()

        if st.button('Logout', use_container_width=True):
            _reset_auth_state()
            st.rerun()

    st.subheader(f'Welcome {current_user} ({current_role}) - Branch: {current_branch}')

    try:
        inventory = load_inventory(db, current_branch)
    except PyMongoError as err:
        st.error(f'Failed to load inventory: {err}')
        return

    search_query = st.text_input('Search Product')
    filtered_items = sorted(inventory.items())
    if search_query.strip():
        query = search_query.strip().lower()
        filtered_items = [(item, qty) for item, qty in filtered_items if query in item.lower()]

    st.markdown(f'### {current_branch} Stock')
    table_rows = [{'Item': item, 'Quantity': qty} for item, qty in filtered_items]
    if table_rows:
        st.dataframe(table_rows, use_container_width=True, hide_index=True)
    else:
        st.info('No inventory items found for this branch.')

    tab_add, tab_delete, tab_transfer, tab_chain = st.tabs([
        'Add / Update Stock',
        'Delete Product',
        'Stock Transfer',
        'Blockchain Ledger'
    ])

    with tab_add:
        with st.form('add_update_form'):
            item = st.text_input('Item Name')
            qty_change = st.number_input('Quantity Change (e.g., 10 or -5)', value=0, step=1)
            add_submitted = st.form_submit_button('Add / Update Stock', type='primary')

        if add_submitted:
            normalized_item = item.strip().title()
            if not normalized_item:
                st.error('Item name cannot be empty.')
            elif int(qty_change) == 0:
                st.info('Quantity change cannot be zero.')
            else:
                ok, message = add_update_stock(
                    db,
                    blockchain,
                    current_user,
                    current_branch,
                    normalized_item,
                    int(qty_change)
                )
                if ok:
                    st.success(message)
                    st.rerun()
                else:
                    st.error(message)

    with tab_delete:
        delete_candidates = sorted(inventory.keys())
        if not delete_candidates:
            st.info('No products available to delete in this branch.')
        else:
            with st.form('delete_product_form'):
                item_to_delete = st.selectbox('Select Product', delete_candidates)
                confirm_delete = st.checkbox('I understand this action cannot be undone.')
                delete_submitted = st.form_submit_button('Delete Product')

            if delete_submitted:
                if not confirm_delete:
                    st.error('Please confirm deletion before proceeding.')
                else:
                    ok, message = delete_product(db, blockchain, current_user, current_branch, item_to_delete)
                    if ok:
                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)

    with tab_transfer:
        target_branches = [branch for branch in ALL_BRANCHES if branch != current_branch]
        transferable_items = sorted([item for item, qty in inventory.items() if qty > 0])

        if not target_branches:
            st.info('No destination branch available for transfer.')
        elif not transferable_items:
            st.info('No in-stock products available for transfer.')
        else:
            with st.form('stock_transfer_form'):
                to_branch = st.selectbox('To Branch', target_branches)
                transfer_item = st.selectbox('Item', transferable_items)
                max_qty = int(inventory.get(transfer_item, 0))
                transfer_qty = st.number_input('Quantity', min_value=1, max_value=max_qty, value=1, step=1)
                transfer_submitted = st.form_submit_button('Confirm Transfer')

            if transfer_submitted:
                ok, message = execute_stock_transfer(
                    db,
                    blockchain,
                    current_user,
                    current_branch,
                    transfer_item,
                    int(transfer_qty),
                    to_branch
                )
                if ok:
                    st.success(message)
                    st.rerun()
                else:
                    st.error(message)

    with tab_chain:
        if current_role != 'admin':
            st.warning('Only admins can view the global blockchain ledger.')
        else:
            try:
                blocks = get_blockchain_records(db, blockchain)
            except PyMongoError as err:
                st.error(f'Failed to load blockchain records: {err}')
                blocks = []

            if not blocks:
                st.info('The global blockchain is empty.')
            else:
                for block in blocks:
                    block_time = datetime.fromtimestamp(float(block['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
                    with st.expander(f"Block {block['index']} - {block_time}"):
                        st.write(f"Previous Hash: {block.get('previous_hash', '')}")
                        st.write(f"Nonce: {block.get('nonce', '')}")
                        if block.get('current_hash'):
                            st.write(f"Current Hash: {block.get('current_hash')}")

                        transactions = sorted(block.get('transactions', []), key=lambda tx: float(tx.get('timestamp', 0)))
                        if not transactions:
                            st.caption('No transactions in this block (Genesis block).')
                        else:
                            tx_rows = []
                            for tx in transactions:
                                tx_rows.append(
                                    {
                                        'Branch': tx.get('branch'),
                                        'User': tx.get('user'),
                                        'Action': tx.get('action'),
                                        'Item': tx.get('item'),
                                        'Quantity': int(tx.get('quantity', 0)),
                                        'Time': datetime.fromtimestamp(float(tx.get('timestamp', 0))).strftime('%Y-%m-%d %H:%M:%S')
                                    }
                                )
                            st.dataframe(tx_rows, use_container_width=True, hide_index=True)


def main():
    st.set_page_config(page_title='Secured Inventory Management', layout='wide')
    st.title('Secured Multi-Branch Inventory Management')

    _initialize_session_state()

    try:
        db, blockchain = initialize_resources()
    except Exception as err:
        st.error(f'Could not initialize application resources: {err}')
        st.stop()

    if not st.session_state.authenticated:
        render_login(db)
        return

    render_inventory_dashboard(db, blockchain)


if __name__ == '__main__':
    main()
