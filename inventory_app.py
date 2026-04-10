import time
import logging
import os
import threading
from typing import Any, Callable, Tuple

import bcrypt
from dotenv import load_dotenv
from tkinter import *
from tkinter import messagebox, ttk

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

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load app env first, then DB-only overrides.
APP_ENV_FILE = os.environ.get('APP_ENV_FILE', '.env')
DB_ENV_FILE = os.environ.get('DB_ENV_FILE', '.env.db')
load_dotenv(APP_ENV_FILE)
load_dotenv(DB_ENV_FILE, override=True)

MONGO_URI = os.environ.get('MONGO_URI')
MONGO_DB_NAME = os.environ.get('MONGO_DB_NAME') or 'inventory_db'
BCRYPT_PREFIXES = ('$2a$', '$2b$', '$2y$')

# Global Mongo Handles
mongo_client = None
mongo_db = None


def _is_mongo_database(candidate: Any) -> bool:
    return (
        candidate is not None
        and hasattr(candidate, 'list_collection_names')
        and hasattr(candidate, 'client')
        and hasattr(candidate, 'name')
    )


def close_db_connection(db: Any):
    # Mongo handle is shared across app and should not be closed per operation.
    return


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


try:
    if MongoClient is None:
        logging.critical('pymongo is not installed. Install dependencies from requirements.txt.')
    elif not MONGO_URI:
        logging.critical('MONGO_URI is missing in environment settings.')
    else:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        mongo_client.admin.command('ping')
        mongo_db = mongo_client[MONGO_DB_NAME]
        _initialize_mongo_collections(mongo_db)
        _seed_default_users_if_empty(mongo_db)
except (PyMongoError, Exception) as pool_err:
    logging.critical(f'Failed to initialize database client: {pool_err}')

def get_db_connection() -> Any:
    """
    Retrieves MongoDB handle.
    """
    return mongo_db

class InventorySystem:
    def __init__(self, root):
        self.root = root
        self.root.title('Multi-Branch Inventory Management System')
        self.root.geometry('900x700')
        self.inventory = {}
        self.current_user = None
        self.current_branch = None
        self.current_role = None # For Role-Based Access Control
        self.all_branches = ["Inventory_1", "Inventory_2"] 
        
        # Use a short-lived connection just for initialization.
        db = get_db_connection()
        if db is None:
            root.withdraw()
            messagebox.showerror("Fatal Error", "Could not connect to database. Check .env file and console.")
            root.quit()
            return
            
        try:
            self.blockchain = Blockchain()
            self.blockchain.load_chain(db)
        except Exception as err:
            messagebox.showerror("Fatal Error", f"Could not load blockchain: {err}")
            root.quit()
            return
        finally:
            close_db_connection(db)
        
        self.search_var = None
        self.search_entry = None
        self.loginscreen()

    def start_thread(self, target_function, args=()):
        """Starts a new daemon thread for background tasks like DB operations."""
        thread = threading.Thread(target=target_function, args=args, daemon=True)
        thread.start()

    def show_message(self, type, title, message, parent=None):
        """Schedules a messagebox to be shown on the main UI thread."""
        if parent is None:
            parent = self.root
        
        if type == 'info':
            self.root.after(0, lambda: messagebox.showinfo(title, message, parent=parent))
        elif type == 'error':
            self.root.after(0, lambda: messagebox.showerror(title, message, parent=parent))

    def clear_root(self):
        """Destroys all widgets in the root window."""
        for widget in self.root.winfo_children():
            widget.destroy()

    def sync_blockchain_cache(self, db):
        """Best-effort resync for in-memory headers after rollbacks or external writes."""
        if db is None:
            return
        try:
            self.blockchain.sync_chain_headers(db)
        except Exception as sync_err:
            logging.warning(f"Could not refresh blockchain cache: {sync_err}")

    def loginscreen(self):
        """Displays the main login UI."""
        # Reset auth state when user returns to the login screen.
        self.current_user = None
        self.current_branch = None
        self.current_role = None

        self.clear_root()
        self.root.configure(bg="#e6f7ff")

        login_frame = Frame(self.root, bg="#d1e7dd", bd=2, relief="solid")
        login_frame.pack(expand=True, padx=50, pady=50)

        Label(
            login_frame,
            text="Multi-Branch Inventory Login",
            font=("Arial", 28, "bold"),
            bg="#d1e7dd",
            fg="#0a3d62",
            pady=16
        ).pack(pady=(10, 20))

        Label(login_frame, text="Select Branch", font=("Arial", 18), bg="#d1e7dd", fg="#303960").pack(pady=5)
        self.branch_var = StringVar()
        branch_combo = ttk.Combobox(
            login_frame,
            textvariable=self.branch_var,
            values=self.all_branches,
            state="readonly",
            font=("Arial", 16)
        )
        branch_combo.pack(pady=8, padx=20, fill="x")
        branch_combo.set(self.all_branches[0])

        Label(login_frame, text="User ID", font=("Arial", 18), bg="#d1e7dd", fg="#303960").pack(pady=5)
        self.user_entry = Entry(login_frame, font=("Arial", 16), bg="#f5f6fa", fg="#222f3e", width=25)
        self.user_entry.pack(ipady=10, pady=8, padx=20)

        Label(login_frame, text="PIN", font=("Arial", 18), bg="#d1e7dd", fg="#303960").pack(pady=5)
        self.pin_entry = Entry(login_frame, font=("Arial", 16), show="*", bg="#f5f6fa", fg="#222f3e", width=25)
        self.pin_entry.pack(ipady=10, pady=8, padx=20)
        
        self.login_button = Button(
            login_frame,
            text="Login",
            font=("Arial", 16, "bold"),
            bg="#62d0ff",
            fg="#182c61",
            command=self.login,
            width=20
        )
        self.login_button.pack(pady=24)
        
        self.root.bind('<Return>', self.login) # Allow pressing Enter to login

        info_frame = Frame(login_frame, bg="#f0f4f7", bd=1, relief="solid")
        info_frame.pack(pady=20, padx=20, fill="x")
        Label(
            info_frame,
            text="Sample Login Credentials (from DB):",
            font=("Arial", 14, "bold"),
            bg="#f0f4f7",
            fg="#303960"
        ).pack(pady=(5,2))
        Label(
            info_frame,
            text="Use your sample users (e.g., admin1/1234)",
            font=("Arial", 12),
            bg="#f0f4f7",
            fg="#555",
            justify=LEFT
        ).pack(pady=(0, 10))

    def login(self, event=None):
        """Handles the login button click and starts the login logic thread."""
        user = self.user_entry.get()
        pin = self.pin_entry.get()
        selected_branch = self.branch_var.get()
        
        if not selected_branch:
            messagebox.showerror('Login Failed', 'Please select a branch')
            return
        if not user or not pin:
            messagebox.showerror('Login Failed', 'User ID and PIN cannot be empty')
            return

        self.login_button.config(state=DISABLED, text="Logging in...")
        self.start_thread(self.login_logic, args=(user, pin, selected_branch))

    def login_logic(self, user, pin, selected_branch):
        """Validates user credentials and fetches their role in a background thread."""
        db = None
        login_success = False
        try:
            db = get_db_connection()
            if db is None:
                self.show_message('error', 'Login Error', 'Could not connect to database.')
                return

            result_doc = db.users.find_one(
                {"username": user},
                {"_id": 0, "pin": 1, "branch": 1, "role": 1}
            )

            result = None
            if result_doc:
                result = (
                    result_doc.get('pin'),
                    result_doc.get('branch'),
                    result_doc.get('role', 'user')
                )
            
            if result:
                stored_pin = result[0]
                user_branch = str(result[1] or '').strip()
                user_role = str(result[2] or 'user').strip().lower()

                pin_valid, pin_needs_upgrade = _verify_user_pin(pin, stored_pin)
                
                # Check PIN and if user is assigned to the selected branch
                if user_branch == selected_branch and pin_valid:
                    self.current_user = user
                    self.current_branch = selected_branch
                    self.current_role = user_role

                    if pin_needs_upgrade:
                        try:
                            upgraded_hash = bcrypt.hashpw(pin.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                            db.users.update_one({'username': user}, {'$set': {'pin': upgraded_hash}})
                        except PyMongoError as upgrade_err:
                            logging.warning('Could not auto-upgrade plaintext PIN for %s: %s', user, upgrade_err)

                    login_success = True
                    self.root.after(0, self.main_screen) # Switch to main screen on UI thread
                else:
                    self.show_message('error', 'Login Failed', 'Invalid credentials or wrong branch')
            else:
                if db.users.count_documents({}) == 0:
                    self.show_message('error', 'Login Failed', 'No users found. Default users are created at startup. Restart the app and try admin1 / 1234.')
                else:
                    self.show_message('error', 'Login Failed', 'Invalid user ID or PIN')
                
        except PyMongoError as err:
            self.show_message('error', 'Login Error', f'A database error occurred: {err}')
        except Exception as e:
            self.show_message('error', 'Login Error', f'An unexpected error occurred: {e}')
        finally:
            # Only re-enable the button if login failed.
            if not login_success:
                self.root.after(0, lambda: self.login_button.config(state=NORMAL, text="Login"))
            close_db_connection(db)


    def main_screen(self):
        """Displays the main inventory management UI."""
        self.clear_root()
        self.root.unbind('<Return>') 

        header_frame = Frame(self.root, bg="#2c3e50")
        header_frame.pack(fill="x", pady=(0, 10))
        Label(
            header_frame,
            text=f'Welcome {self.current_user} ({self.current_role}) - Branch: {self.current_branch}',
            font=('Arial', 20, 'bold'),
            bg="#2c3e50",
            fg="white",
            pady=10
        ).pack()

        inventory_frame = Frame(self.root)
        inventory_frame.pack(pady=10, padx=20, fill="both", expand=True)
        Label(inventory_frame, text=f'{self.current_branch} Stock', font=('Arial', 18, 'bold')).pack()

        # ----------------- CUSTOMIZE TREEVIEW FONT ----------------------
        style = ttk.Style()
        # Set the font to be BOLD only (Standard size 11, instead of 14)
        style.configure("Treeview", 
                        font=('Arial', 11, 'bold'), 
                        rowheight=25) 
        
        style.configure("Treeview.Heading", 
                        font=('Arial', 12, 'bold'))
        # -----------------------------------------------------------------

        self.tree = ttk.Treeview(inventory_frame, columns=('Item', 'Quantity'), show='headings', height=15)
        self.tree.heading('Item', text='Item')
        self.tree.heading('Quantity', text='Quantity')
        self.tree.column('Item', width=400)
        self.tree.column('Quantity', width=100, anchor="center")
        
        scrollbar = ttk.Scrollbar(inventory_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        self.tree.bind('<<TreeviewSelect>>', self.on_row_select)

        # Load inventory in a background thread
        self.start_thread(self.load_inventory_logic)

        search_frame = Frame(self.root, bg="#ecf0f1", bd=2, relief="groove")
        search_frame.pack(pady=8, padx=20, fill="x")
        Label(search_frame, text='Search Product', bg="#ecf0f1",
              font=('Arial', 12)).grid(row=0, column=0, padx=5, pady=6, sticky="w")
        self.search_var = StringVar()
        self.search_entry = Entry(search_frame, textvariable=self.search_var, font=('Arial', 12))
        self.search_entry.grid(row=0, column=1, padx=5, pady=6, sticky="ew")
        Button(search_frame, text='Clear', command=self.clear_search,
               bg="#95a5a6", fg="white", font=('Arial', 11, 'bold')).grid(row=0, column=2, padx=5, pady=6)
        search_frame.grid_columnconfigure(1, weight=1)
        self.search_entry.bind('<KeyRelease>', self.on_search_key) # Search as you type

        input_frame = Frame(self.root, bg="#ecf0f1", bd=2, relief="raised")
        input_frame.pack(pady=10, padx=20, fill="x")
        Label(input_frame, text='Item Name', bg="#ecf0f1", font=('Arial', 12)).grid(
            row=0, column=0, padx=5, pady=5, sticky="w"
        )
        self.item_entry = Entry(input_frame, font=('Arial', 12))
        self.item_entry.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        Label(input_frame, text='Quantity (e.g., 10 or -5)', bg="#ecf0f1", font=('Arial', 12)).grid(
            row=1, column=0, padx=5, pady=5, sticky="w"
        )
        self.qty_entry = Entry(input_frame, font=('Arial', 12))
        self.qty_entry.grid(row=1, column=1, padx=5, pady=5, sticky="ew")
        input_frame.grid_columnconfigure(1, weight=1)

        button_frame = Frame(self.root)
        button_frame.pack(pady=10)
        
        Button(
            button_frame,
            text='Add/Update Stock',
            command=self.add_update_stock,
            bg="#27ae60", fg="white", font=('Arial', 12, 'bold')
        ).pack(side="left", padx=5)
        
        Button(
            button_frame,
            text='Stock Transfer',
            command=self.open_stock_transfer_window,
            bg="#8e44ad", fg="white", font=('Arial', 12, 'bold')
        ).pack(side="left", padx=5)
        
        Button(
            button_frame,
            text='Delete Product',
            command=self.delete_product,
            bg="#e74c3c", fg="white", font=('Arial', 12, 'bold')
        ).pack(side="left", padx=5)

        # Only show 'View Blockchain' button if user is an admin
        if self.current_role == 'admin':
            Button(
                button_frame,
                text='View Blockchain',
                command=self.view_blockchain, 
                bg="#3498db", fg="white", font=('Arial', 12, 'bold')
            ).pack(side="left", padx=5)
            
        Button(
            button_frame,
            text='Switch Branch',
            command=self.loginscreen,
            bg="#f39c12", fg="white", font=('Arial', 12, 'bold')
        ).pack(side="left", padx=5)
        
        Button(
            button_frame,
            text='Logout',
            command=self.root.quit,
            bg="#7f8c8d", fg="white", font=('Arial', 12, 'bold')
        ).pack(side="left", padx=5)
        
    def on_row_select(self, event=None):
        """Populates the item name entry when a row in the tree is clicked."""
        try:
            selected_item = self.tree.selection()[0]
            item_name = self.tree.item(selected_item, 'values')[0]
            
            self.item_entry.delete(0, END)
            self.qty_entry.delete(0, END)
            
            self.item_entry.insert(0, item_name)
        except IndexError:
            pass # Ignore if no row is selected or selection is cleared

    def load_inventory_logic(self):
        """Fetches the current branch's inventory from the DB in a thread."""
        db = None
        try:
            db = get_db_connection()
            if db is None:
                self.show_message('error', 'Load Error', 'Failed to connect to DB for inventory load.')
                return

            data = db.inventory.find(
                {'branch': self.current_branch},
                {'_id': 0, 'item': 1, 'quantity': 1}
            )
            self.inventory = {
                entry.get('item'): int(entry.get('quantity', 0))
                for entry in data
                if entry.get('item')
            }
            
            # Schedule the UI update on the main thread
            self.root.after(0, self.load_inventory_display)
        except PyMongoError as err:
            self.show_message('error', 'Load Error', f'Failed to load inventory: {err}')
        finally:
            close_db_connection(db)

    def load_inventory_display(self):
        """Clears and repopulates the Treeview with current inventory data."""
        for i in self.tree.get_children():
            self.tree.delete(i)
        
        sorted_items = sorted(self.inventory.items())
        
        for item, qty in sorted_items:
            self.tree.insert('', END, values=(item, qty))

    def save_inventory_item_logic(self, db, item, qty, branch, session=None):
        """
        Saves a single item's quantity to MongoDB.
        This function assumes it's part of a larger transaction and does NOT commit.
        """
        db.inventory.update_one(
            {'item': item, 'branch': branch},
            {'$set': {'item': item, 'branch': branch, 'quantity': int(qty)}},
            upsert=True,
            session=session
        )

    def add_update_stock(self):
        """Validates input and starts the add/update stock background thread."""
        item = self.item_entry.get().strip().title()
        
        try:
            qty_change = int(self.qty_entry.get())
        except Exception:
            messagebox.showerror('Invalid Input', 'Quantity must be an integer (e.g., 10 or -5)')
            return

        if not item:
            messagebox.showerror('Invalid Input', 'Item name cannot be empty')
            return
            
        if qty_change == 0:
            messagebox.showinfo('No Change', 'Quantity change cannot be zero.')
            return

        self.start_thread(self.add_update_stock_logic, args=(item, qty_change))

    def add_update_stock_logic(self, item, qty_change):
        """
        Handles the logic for adding/updating stock in a transaction:
        1. Validate using a quick read.
        2. Mine and stage blockchain block.
        3. Re-check stock and update inventory.
        """
        db = None
        mined_block_index = None
        operation_committed = False
        try:
            db = get_db_connection()
            if db is None:
                self.show_message('error', 'Error', 'Could not connect to database for update.')
                return

            preview_doc = db.inventory.find_one(
                {'item': item, 'branch': self.current_branch},
                {'_id': 0, 'quantity': 1}
            )
            preview_qty = int(preview_doc.get('quantity', 0)) if preview_doc else 0

            preview_new_qty = preview_qty + qty_change

            if preview_new_qty < 0:
                self.show_message('error', 'Invalid Quantity', f'Cannot remove {abs(qty_change)} units. Only {preview_qty} units of "{item}" are in stock.')
                return

            transactions_batch = [{
                'user': self.current_user,
                'action': 'Add/Update',
                'item': item,
                'quantity': qty_change, 
                'timestamp': time.time(),
                'branch': self.current_branch
            }]

            def mongo_operation(session):
                nonlocal mined_block_index

                logging.info(f"Mining new block for item {item}...")
                block = self.blockchain.mine_and_create_block(db, transactions_batch, session=session)
                mined_block_index = block['index']
                logging.info(f"Block mined and staged! Index: {block['index']} | Nonce: {block['nonce']}")

                if qty_change < 0:
                    update_result = db.inventory.update_one(
                        {
                            'item': item,
                            'branch': self.current_branch,
                            'quantity': {'$gte': abs(qty_change)}
                        },
                        {'$inc': {'quantity': qty_change}},
                        session=session
                    )
                    if update_result.modified_count == 0:
                        raise ValueError(f'Stock changed during processing. Units for "{item}" are no longer sufficient.')
                else:
                    db.inventory.update_one(
                        {'item': item, 'branch': self.current_branch},
                        {
                            '$inc': {'quantity': qty_change},
                            '$setOnInsert': {'item': item, 'branch': self.current_branch}
                        },
                        upsert=True,
                        session=session
                    )

                current_doc = db.inventory.find_one(
                    {'item': item, 'branch': self.current_branch},
                    {'_id': 0, 'quantity': 1},
                    session=session
                )
                return int(current_doc.get('quantity', 0)) if current_doc else 0

            new_qty, _ = run_mongo_transaction(db, mongo_operation)
            operation_committed = True

            # Update in-memory inventory
            self.inventory[item] = new_qty
            
            # Update UI from the main thread
            self.root.after(0, self.on_search_key) # Refreshes the tree view with filters
            self.root.after(0, lambda: self.item_entry.delete(0, END))
            self.root.after(0, lambda: self.qty_entry.delete(0, END))
            
            if qty_change > 0:
                self.show_message('info', 'Success', f'Added {qty_change} units to "{item}". New total: {new_qty}.')
            else:
                self.show_message('info', 'Success', f'Removed {abs(qty_change)} units from "{item}". New total: {new_qty}.')

        except PyMongoError as err:
            if db is not None:
                if mined_block_index is not None and not operation_committed:
                    try:
                        self.blockchain.rollback_block(db, mined_block_index)
                    except Exception:
                        pass
            self.show_message('error', 'Error', f'Database error: {err}\nTransaction rolled back.')
        except Exception as e:
            if db is not None:
                if mined_block_index is not None and not operation_committed:
                    try:
                        self.blockchain.rollback_block(db, mined_block_index)
                    except Exception:
                        pass
            self.show_message('error', 'Error', f'An unexpected error occurred: {e}\nTransaction rolled back.')
        finally:
            close_db_connection(db)

    def delete_product(self):
        """Validates selection and starts the delete product background thread."""
        selected = self.tree.selection()
        if not selected:
            messagebox.showerror('No Selection', 'Select a product row to delete')
            return

        item_vals = self.tree.item(selected[0], 'values')
        if not item_vals:
            messagebox.showerror('Error', 'Unable to read selected item')
            return

        item_name = item_vals[0]
        if not messagebox.askyesno('Confirm Delete', f'Delete product "{item_name}" from {self.current_branch}?\nThis cannot be undone and will be logged.'):
            return
            
        self.start_thread(self.delete_product_logic, args=(item_name, selected[0]))

    def delete_product_logic(self, item_name, tree_item_id):
        """
        Handles the logic for deleting a product in a transaction:
        1. Verify product still exists.
        2. Mine and stage blockchain block.
        3. Delete from inventory.
        """
        db = None
        mined_block_index = None
        operation_committed = False
        try:
            db = get_db_connection()
            if db is None:
                self.show_message('error', 'Error', 'Could not connect to database for delete.')
                return

            existing = db.inventory.find_one(
                {'item': item_name, 'branch': self.current_branch},
                {'_id': 0, 'quantity': 1}
            )
            if not existing:
                self.show_message('error', 'Error', f'Product "{item_name}" no longer exists in {self.current_branch}.')
                return

            transactions_batch = [{
                'user': self.current_user,
                'action': 'Delete Product',
                'item': item_name,
                'quantity': 0,
                'timestamp': time.time(),
                'branch': self.current_branch
            }]

            def mongo_operation(session):
                nonlocal mined_block_index
                logging.info(f"Mining new block for deleting {item_name}...")
                block = self.blockchain.mine_and_create_block(db, transactions_batch, session=session)
                mined_block_index = block['index']
                logging.info(f"Block mined and staged! Index: {block['index']} | Nonce: {block['nonce']}")

                delete_result = db.inventory.delete_one(
                    {'item': item_name, 'branch': self.current_branch},
                    session=session
                )
                if delete_result.deleted_count == 0:
                    raise ValueError(f'Product "{item_name}" no longer exists in {self.current_branch}.')

            run_mongo_transaction(db, mongo_operation)
            operation_committed = True

            if item_name in self.inventory:
                del self.inventory[item_name]
                
            # Update UI from main thread
            self.root.after(0, lambda: self.tree.delete(tree_item_id))
            self.show_message('info', 'Deleted', f'Product "{item_name}" deleted successfully from {self.current_branch}')
            
        except PyMongoError as err:
            if db is not None:
                if mined_block_index is not None and not operation_committed:
                    try:
                        self.blockchain.rollback_block(db, mined_block_index)
                    except Exception:
                        pass
            self.show_message('error', 'Error', f'Failed to delete product: {err}\nTransaction rolled back.')
        except Exception as e:
            if db is not None:
                if mined_block_index is not None and not operation_committed:
                    try:
                        self.blockchain.rollback_block(db, mined_block_index)
                    except Exception:
                        pass
            self.show_message('error', 'Error', f'An unexpected error occurred: {e}\nTransaction rolled back.')
        finally:
            close_db_connection(db)

    def view_blockchain(self):
        """
        Displays the blockchain ledger in a new window.
        Fetches full block data (with TXs) on-demand using a new, short-lived connection.
        """
        if self.current_role != 'admin':
            messagebox.showerror('Access Denied', 'Only admins can view the global blockchain ledger.')
            return
            
        blocks_text = 'Global Blockchain Ledger (All Branches)\n' + '=' * 50 + '\n\n'
        
        db = None
        try:
            db = get_db_connection()
            if db is None:
                self.show_message('error', 'Blockchain Error', 'Could not connect to database to view ledger.')
                return

            self.blockchain.sync_chain_headers(db)

            if not self.blockchain.chain:
                messagebox.showinfo('Blockchain', 'The Global Blockchain is empty.')
                return

            for block_header in self.blockchain.chain[:]:
                block = self.blockchain.get_block_with_transactions(db, block_header['index'])

                if block is None:
                    blocks_text += f"Block {block_header['index']} - ERROR: FAILED TO LOAD TRANSACTIONS\n"
                    blocks_text += f"Previous Hash: {block_header['previous_hash']}\n"
                    blocks_text += '\n' + '-'*60 + '\n\n'
                    continue

                blocks_text += f"Block {block['index']} - Timestamp: {time.ctime(block['timestamp'])}\n"
                blocks_text += f"Previous Hash: {block['previous_hash']}\n"
                blocks_text += f"Nonce: {block['nonce']}\n"
                blocks_text += "Transactions:\n"

                if not block['transactions']:
                    blocks_text += "   - No transactions in this block (Genesis block)\n"

                sorted_txs = sorted(block.get('transactions', []), key=lambda x: x['timestamp'])

                for tx in sorted_txs:
                    blocks_text += (
                        f" - Branch: {tx['branch']:<12} | User: {tx['user']:<10} | Action: {tx['action']:<15} | "
                        f"Item: {tx['item']:<20} | Qty: {tx['quantity']:<5} | "
                        f"Time: {time.ctime(tx['timestamp'])}\n"
                    )
                blocks_text += '\n' + '-'*60 + '\n\n'

        except PyMongoError as err:
            self.show_message('error', 'Blockchain Error', f'Failed to read blockchain from DB: {err}')
            return
        finally:
            close_db_connection(db)
        
        # Display the blockchain in a new Toplevel window
        blockchain_window = Toplevel(self.root)
        blockchain_window.title('Global Blockchain Ledger')
        blockchain_window.geometry('950x600')
        text_frame = Frame(blockchain_window)
        text_frame.pack(fill="both", expand=True, padx=10, pady=10)
        txt = Text(text_frame, wrap=NONE, width=80, height=30, font=('Courier', 10))
        v_scrollbar = Scrollbar(text_frame, orient="vertical", command=txt.yview)
        h_scrollbar = Scrollbar(text_frame, orient="horizontal", command=txt.xview)
        txt.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        txt.insert(END, blocks_text)
        txt.config(state=DISABLED) # Make text read-only
        v_scrollbar.pack(side="right", fill="y")
        h_scrollbar.pack(side="bottom", fill="x")
        txt.pack(side="left", fill="both", expand=True)

    def open_stock_transfer_window(self):
        """Opens a modal window for transferring stock to another branch."""
        transfer_window = Toplevel(self.root)
        transfer_window.title('Stock Transfer')
        transfer_window.geometry('450x350')
        transfer_window.configure(bg="#f0f4f7")
        transfer_window.grab_set() # Make the window modal

        form_frame = Frame(transfer_window, bg="#f0f4f7", pady=15, padx=15)
        form_frame.pack(expand=True, fill="both")

        Label(form_frame, text=f"From Branch: {self.current_branch}", font=('Arial', 12, 'bold'), bg="#f0f4f7").grid(row=0, column=0, columnspan=3, pady=5, sticky="w")
        Label(form_frame, text="To Branch:", font=('Arial', 12), bg="#f0f4f7").grid(row=1, column=0, pady=5, sticky="w")
        
        target_branches = [b for b in self.all_branches if b != self.current_branch]
        to_branch_var = StringVar()
        to_branch_combo = ttk.Combobox(
            form_frame, textvariable=to_branch_var, values=target_branches,
            state="readonly", font=("Arial", 12)
        )
        to_branch_combo.grid(row=1, column=1, pady=5, sticky="ew", columnspan=2)
        if target_branches:
            to_branch_combo.set(target_branches[0])

        Label(form_frame, text="Item:", font=('Arial', 12), bg="#f0f4f7").grid(row=2, column=0, pady=5, sticky="w")
        
        # Only list items that are in stock
        available_items = sorted([item for item, qty in self.inventory.items() if qty > 0])
        item_var = StringVar()
        item_combo = ttk.Combobox(
            form_frame, textvariable=item_var, values=available_items,
            state="readonly", font=("Arial", 12)
        )
        item_combo.grid(row=2, column=1, pady=5, sticky="ew")

        stock_label = Label(form_frame, text="(In Stock: --)", font=('Arial', 10, 'italic'), bg="#f0f4f7")
        stock_label.grid(row=2, column=2, padx=5, sticky="w")
        
        def on_item_select(event=None):
            """Updates the 'In Stock' label when an item is selected."""
            selected_item = item_var.get()
            current_stock = self.inventory.get(selected_item, 0)
            stock_label.config(text=f"(In Stock: {current_stock})")
        item_combo.bind("<<ComboboxSelected>>", on_item_select)

        Label(form_frame, text="Quantity:", font=('Arial', 12), bg="#f0f4f7").grid(row=3, column=0, pady=5, sticky="w")
        qty_entry = Entry(form_frame, font=("Arial", 12))
        qty_entry.grid(row=3, column=1, pady=5, sticky="ew")

        form_frame.grid_columnconfigure(1, weight=1)

        confirm_btn = Button(
            form_frame,
            text="Confirm Transfer",
            font=('Arial', 12, 'bold'),
            bg="#27ae60", fg="white",
            command=lambda: self.execute_stock_transfer(
                item_var.get(),
                qty_entry.get(),
                to_branch_var.get(),
                transfer_window
            )
        )
        confirm_btn.grid(row=4, column=0, columnspan=3, pady=20)


    def execute_stock_transfer(self, item, qty_str, to_branch, window):
        """Validates transfer input and starts the transfer logic thread."""
        item = item.strip().title()

        if not item or not to_branch:
            messagebox.showerror('Invalid Input', 'Please select an item and a target branch.', parent=window)
            return
            
        try:
            quantity = int(qty_str)
            if quantity <= 0:
                raise ValueError("Quantity must be positive")
        except ValueError:
            messagebox.showerror('Invalid Input', 'Quantity must be a positive integer.', parent=window)
            return

        current_stock = self.inventory.get(item, 0)
        if quantity > current_stock:
            messagebox.showerror('Insufficient Stock', f'Cannot transfer {quantity} units. Only {current_stock} units of "{item}" are in {self.current_branch}.', parent=window)
            return

        if not messagebox.askyesno('Confirm Transfer', f'Transfer {quantity} units of "{item}" from {self.current_branch} to {to_branch}?', parent=window):
            return

        self.start_thread(self.execute_stock_transfer_logic, args=(item, quantity, to_branch, window))

    def execute_stock_transfer_logic(self, item, quantity, to_branch, window):
        """
        Handles the logic for a stock transfer in a single transaction:
        1. Pre-check source stock.
        2. Mine and stage transfer block.
        3. Lock source and target rows.
        4. Re-check stock and update both records.
        5. Commit transaction.
        """
        db = None
        mined_block_index = None
        operation_committed = False
        try:
            db = get_db_connection()
            if db is None:
                self.show_message('error', 'Transfer Failed', 'Could not connect to database for transfer.', parent=window)
                return

            preview_doc = db.inventory.find_one(
                {'item': item, 'branch': self.current_branch},
                {'_id': 0, 'quantity': 1}
            )
            preview_stock_source = int(preview_doc.get('quantity', 0)) if preview_doc else 0
            if quantity > preview_stock_source:
                self.show_message('error', 'Insufficient Stock', f'Only {preview_stock_source} units are currently available.', parent=window)
                return

            tx_time = time.time()
            transactions_batch = [
                {
                    'user': self.current_user, 'action': 'Transfer Out', 'item': item,
                    'quantity': -quantity, 'timestamp': tx_time, 'branch': self.current_branch
                },
                {
                    'user': self.current_user, 'action': 'Transfer In', 'item': item,
                    'quantity': quantity, 'timestamp': tx_time + 0.001, 'branch': to_branch
                }
            ]

            def mongo_operation(session):
                nonlocal mined_block_index
                logging.info(f"Mining new block for transfer of {item}...")
                block = self.blockchain.mine_and_create_block(db, transactions_batch, session=session)
                mined_block_index = block['index']
                logging.info(f"Block mined and staged! Index: {block['index']} | Nonce: {block['nonce']}")

                source_result = db.inventory.update_one(
                    {
                        'item': item,
                        'branch': self.current_branch,
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

                updated_source = db.inventory.find_one(
                    {'item': item, 'branch': self.current_branch},
                    {'_id': 0, 'quantity': 1},
                    session=session
                )
                return int(updated_source.get('quantity', 0)) if updated_source else 0

            new_stock_source, _ = run_mongo_transaction(db, mongo_operation)
            operation_committed = True
            
            # Update local in-memory inventory
            self.inventory[item] = new_stock_source
            
            # Update UI from the main thread
            self.root.after(0, self.on_search_key) # Refresh main tree
            self.root.after(0, window.destroy)
            self.show_message('info', 'Success', f'Transferred {quantity} units of "{item}" to {to_branch} successfully.')

        except PyMongoError as err:
            if db is not None:
                if mined_block_index is not None and not operation_committed:
                    try:
                        self.blockchain.rollback_block(db, mined_block_index)
                    except Exception:
                        pass
            self.show_message('error', 'Transfer Failed', f'An error occurred: {err}\nTransaction rolled back.', parent=window)
        except Exception as e:
            if db is not None:
                if mined_block_index is not None and not operation_committed:
                    try:
                        self.blockchain.rollback_block(db, mined_block_index)
                    except Exception:
                        pass
            self.show_message('error', 'Transfer Failed', f'An unexpected error occurred: {e}\nTransaction rolled back.', parent=window)
        finally:
            close_db_connection(db)


    def on_search_key(self, event=None):
        """Filters the Treeview in real-time based on the search box text."""
        query = (self.search_var.get() if self.search_var else '').strip().lower()
        
        # Clear the tree
        for iid in self.tree.get_children():
            self.tree.delete(iid)
            
        if not query:
            # If search is empty, reload the full display
            self.load_inventory_display()
            return
            
        # Filter in-memory inventory
        filtered_items = sorted([
            (item, qty) for item, qty in self.inventory.items() 
            if query in item.lower()
        ])
        
        # Repopulate tree with filtered items
        for item, qty in filtered_items:
            self.tree.insert('', END, values=(item, qty))

    def clear_search(self):
        """Clears the search box and reloads the full inventory display."""
        if self.search_var:
            self.search_var.set('')
        self.load_inventory_display()


if __name__ == '__main__':
    if not (mongo_db is not None and bool(MONGO_URI)):
        logging.critical('MongoDB configuration not found or connection failed.')
        logging.info('Set MONGO_URI and optional MONGO_DB_NAME in your environment files.')
        raise SystemExit(1)

    root = Tk()
    app = InventorySystem(root)
    root.mainloop()

    logging.info("Application closed.")