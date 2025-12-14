# -*- coding: utf-8 -*-
import hashlib
import telebot
import subprocess
import os
import json
import zipfile
import tempfile
import shutil
from telebot import types
import time
from datetime import datetime, timedelta
import psutil
import logging
import threading
import sys
import atexit
import requests
import re
# 👇 Imports check karein
from pymongo import MongoClient
import certifi
import dns.resolver  # <--- Ye naya add kiya hai DNS fix ke liye

# --- Flask Keep Alive (Auto-Port Fix) ---
from flask import Flask
from threading import Thread
import socket
import sys

app = Flask('')

@app.route('/')
def home():
    return "I'am RAJPUT File Host - Running Successfully!"

def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('0.0.0.0', port)) == 0

def run_flask():
    # Start checking from Port 8080 or Environment Port
    start_port = int(os.environ.get("PORT", 8080))
    
    # Try up to 10 ports (e.g., 8080 to 8090)
    for port in range(start_port, start_port + 11):
        if not is_port_in_use(port):
            try:
                # Port mil gaya, ab run karein
                print(f"✅ Flask Server running on Port {port}")
                app.run(host='0.0.0.0', port=port)
                return
            except OSError as e:
                if "Address already in use" in str(e):
                    print(f"⚠️ Port {port} busy, trying next...")
                    continue
        else:
            print(f"⚠️ Port {port} is busy. Checking next...")
    
    print("❌ Could not find an open port for Flask.")

def keep_alive():
    t = Thread(target=run_flask)
    t.daemon = True
    t.start()
# --- End Flask Keep Alive ---

# --- Logging Setup (Moved to TOP) ---
# Logger ko sabse pehle initialize karna zaroori hai
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 🛡️ SECURITY SYSTEM 🛡️ ---
RISKY_KEYWORDS = [
    "rm -rf", "shutil.rmtree", "os.remove", "os.rmdir", # Delete
    "os.system", "subprocess.call", "subprocess.Popen", # System
    "os.walk", "glob.glob", # Scanning
    "/storage", "/data/data", "/sdcard", "/etc/passwd", # Paths
    "eval(", "exec(", "bot_token", "api_id", "api_hash", # Hack
    "telegram.Bot", "Client(", "sudo", "chmod"
]

def scan_content_for_risk(content):
    if not content: return False, "No Content"
    try:
        text = content.decode('utf-8', errors='ignore') if isinstance(content, bytes) else str(content)
        for keyword in RISKY_KEYWORDS:
            if keyword in text: return True, keyword
    except: pass
    return False, None

# --- Configuration ---
TOKEN = '8329796864:AAFfQxQHUXQ6W7BJoz82pEudpfvvuZGuAto' # Apna Token Check karein
OWNER_ID = 7692672287
ADMIN_ID = 7692672287
YOUR_USERNAME = 'MS_HAC4KER'
UPDATE_CHANNEL = 'https://t.me/PHANTOM_CODERS'

# Folder setup
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_BOTS_DIR = os.path.join(BASE_DIR, 'upload_bots')
os.makedirs(UPLOAD_BOTS_DIR, exist_ok=True)

# 👇 Apna MongoDB URL yahan daalein 👇
MONGO_URL = "mongodb+srv://mohans78425_db_user:mohans78425_db_user@cluster0.fvfjtik.mongodb.net/?appName=Cluster0"

# --- MongoDB Connection Setup (Fixed for Termux) ---
try:
    # 🛠️ Termux DNS Fix: Manually set DNS to Google (8.8.8.8)
    dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
    dns.resolver.default_resolver.nameservers = ['8.8.8.8']
    
    # Ab connect karein
    mongo_client = MongoClient(MONGO_URL, tlsCAFile=certifi.where())
    db = mongo_client['RajputFileHostDB']
    
    col_files = db['files']
    col_subs = db['subscriptions']
    col_config = db['config']
    
    # Connection test karne ke liye ek choti command
    mongo_client.admin.command('ping')
    logger.info("✅ Connected to MongoDB Successfully!")
except Exception as e:
    logger.error(f"❌ MongoDB Connection Failed: {e}")
    # Agar fail ho jaye to variables ko None set karein taaki bot crash na ho
    mongo_client = None
    db = None

# File upload limits
FREE_USER_LIMIT = 3
SUBSCRIBED_USER_LIMIT = 15
ADMIN_LIMIT = 999
OWNER_LIMIT = float('inf')

# Initialize bot
bot = telebot.TeleBot(TOKEN)

# --- Data structures ---
bot_scripts = {} # Stores info about running scripts {script_key: info_dict}
user_subscriptions = {} # {user_id: {'expiry': datetime_object}}
user_files = {} # {user_id: [(file_name, file_type), ...]}
active_users = set() # Set of all user IDs that have interacted with the bot
admin_ids = {ADMIN_ID, OWNER_ID} # Set of admin IDs
bot_locked = False
# free_mode = False # Removed free_mode

# --- Logging Setup ---
# Configure basic logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Command Button Layouts (ReplyKeyboardMarkup) ---
COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📢 Updates Channel", "ℹ️ Help & Guide"], # ✅ Yahan Add kiya hai
    ["📤 Upload File", "📂 Check Files"],
    ["⚡ Bot Speed", "📊 Statistics"],
    ["📞 Contact Owner"]
]

ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📢 Updates Channel", "ℹ️ Help & Guide"], # ✅ Yahan bhi Add kiya hai
    ["📤 Upload File", "📂 Check Files"],
    ["⚡ Bot Speed", "📊 Statistics"],
    ["💳 Subscriptions", "📢 Broadcast"],
    ["🔒 Lock Bot", "🟢 Running All Code"],
    ["👑 Admin Panel", "🗃️ User Files (Admin)"],
    ["📞 Contact Owner"]
]

# --- MongoDB Load Functions ---
# 👇 Ye Code Paste Karna Hai 👇

def get_user_folder(user_id):
    user_folder = os.path.join(UPLOAD_BOTS_DIR, str(user_id))
    os.makedirs(user_folder, exist_ok=True)
    return user_folder

# 👆 Ye Code Paste Karna Hai 👆

def load_data_from_mongo():
    """Load data from MongoDB AND Restore Zip + Install Requirements on Restart"""
    if db is None: return

    logger.info("📥 Loading data from MongoDB...")
    
    # 1. Load Subscriptions
    for doc in col_subs.find():
        try:
            uid = doc['_id']
            user_subscriptions[uid] = {'expiry': datetime.fromisoformat(doc['expiry'])}
        except: pass

    # 2. Load User Files & Restore Full Environment
    for doc in col_files.find():
        try:
            uid = doc['_id']
            user_folder = get_user_folder(uid)
            loaded_files = []
            
            for file_data in doc.get('files_data', []): 
                fname = file_data.get('name')
                ftype = file_data.get('type')
                content = file_data.get('content')
                
                # Zip Data Check
                zip_name = file_data.get('zip_name')
                zip_content = file_data.get('zip_content')
                
                restored = False
                
                # --- SCENARIO A: ZIP FILE RESTORE ---
                if zip_name and zip_content:
                    try:
                        logger.info(f"📦 Found Zip Backup '{zip_name}' for {uid}. Restoring...")
                        zip_path = os.path.join(user_folder, zip_name)
                        
                        # 1. Zip write karo
                        with open(zip_path, 'wb') as f:
                            f.write(zip_content)
                        
                        # 2. Extract karo
                        with zipfile.ZipFile(zip_path, 'r') as z:
                            z.extractall(user_folder)
                        
                        # 3. AUTOMATIC INSTALL REQUIREMENTS ON RESTART (Ye Zaroori Hai)
                        req_path = os.path.join(user_folder, 'requirements.txt')
                        if os.path.exists(req_path):
                            logger.info(f"🔄 Auto-Installing requirements for {uid} after restart...")
                            try:
                                subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", req_path])
                                logger.info(f"✅ Requirements installed for {uid}")
                            except Exception as e:
                                logger.error(f"❌ Failed to install reqs for {uid}: {e}")

                        restored = True
                    except Exception as e:
                        logger.error(f"Failed to restore zip {zip_name}: {e}")

                # --- SCENARIO B: NORMAL FILE RESTORE ---
                if not restored and fname and content:
                    file_path = os.path.join(user_folder, fname)
                    if not os.path.exists(file_path):
                        try:
                            with open(file_path, 'w', encoding='utf-8', errors='ignore') as f:
                                f.write(content)
                        except: pass
                
                if fname: loaded_files.append((fname, ftype))
            
            if loaded_files: user_files[uid] = loaded_files
                
        except Exception as e:
            logger.error(f"Error loading files for user: {e}")

    # 3. Load Active Users & Admins
    try:
        config_doc = col_config.find_one({'_id': 'global_data'})
        if config_doc:
            active_users.update(set(config_doc.get('active_users', [])))
            admin_ids.update(set(config_doc.get('admins', [])))
            global bot_locked
            bot_locked = config_doc.get('bot_locked', False)
    except: pass

    admin_ids.add(OWNER_ID)
    if ADMIN_ID != OWNER_ID: admin_ids.add(ADMIN_ID)
    
    logger.info(f"✅ Data Loaded: {len(active_users)} Users, {len(user_files)} User Folders")

# Startup par load karein
load_data_from_mongo()

# --- Helper Functions ---
# 👆 Yahan tak 👆
# 👇 YAHAN SE COPY KARO 👇

# --- FSub Config (Yahan apni details daalo) ---
FSUB_CH_1_ID = "@snxrajput_bio"  # ⚠️ Yahan @RawDataBot wali ID daalna (Private Channel)
FSUB_CH_2_ID = "@snnetwork7"  # Public Channel Username
FSUB_LINK_1 = "https://t.me/snxrajput_bio"
FSUB_LINK_2 = "https://t.me/snnetwork7"

def is_user_joined(user_id):
    if user_id in admin_ids: return True # Admin/Owner ko check nahi karega
    try:
        # Dono channel check karega
        s1 = bot.get_chat_member(FSUB_CH_1_ID, user_id).status
        s2 = bot.get_chat_member(FSUB_CH_2_ID, user_id).status
        if s1 in ['left', 'kicked'] or s2 in ['left', 'kicked']: return False
        return True
    except: return True # Agar error aaye to user ko mat roko

def get_fsub_markup():
    markup = types.InlineKeyboardMarkup(row_width=2)
    btn1 = types.InlineKeyboardButton("📢 Join Channel 1", url=FSUB_LINK_1)
    btn2 = types.InlineKeyboardButton("📢 Join Channel 2", url=FSUB_LINK_2)
    markup.add(btn1, btn2)
    markup.add(types.InlineKeyboardButton("✅ Verify & Continue", callback_data="verify_join"))
    return markup

# 👆 YAHAN TAK COPY KARO 👆

def get_user_folder(user_id):
    """Get or create user's folder for storing files"""
    user_folder = os.path.join(UPLOAD_BOTS_DIR, str(user_id))
    os.makedirs(user_folder, exist_ok=True)
    return user_folder

def get_user_file_limit(user_id):
    """Get the file upload limit for a user"""
    # if free_mode: return FREE_MODE_LIMIT # Removed free_mode check
    if user_id == OWNER_ID: return OWNER_LIMIT
    if user_id in admin_ids: return ADMIN_LIMIT
    if user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now():
        return SUBSCRIBED_USER_LIMIT
    return FREE_USER_LIMIT

def get_user_file_count(user_id):
    """Get the number of files uploaded by a user"""
    return len(user_files.get(user_id, []))

def is_bot_running(script_owner_id, file_name): # Parameter renamed for clarity
    """Check if a bot script is currently running for a specific user"""
    script_key = f"{script_owner_id}_{file_name}" # Key uses script_owner_id
    script_info = bot_scripts.get(script_key)
    if script_info and script_info.get('process'):
        try:
            proc = psutil.Process(script_info['process'].pid)
            is_running = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
            if not is_running:
                logger.warning(f"Process {script_info['process'].pid} for {script_key} found in memory but not running/zombie. Cleaning up.")
                if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
                    try:
                        script_info['log_file'].close()
                    except Exception as log_e:
                        logger.error(f"Error closing log file during zombie cleanup {script_key}: {log_e}")
                if script_key in bot_scripts:
                    del bot_scripts[script_key]
            return is_running
        except psutil.NoSuchProcess:
            logger.warning(f"Process for {script_key} not found (NoSuchProcess). Cleaning up.")
            if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
                try:
                     script_info['log_file'].close()
                except Exception as log_e:
                     logger.error(f"Error closing log file during cleanup of non-existent process {script_key}: {log_e}")
            if script_key in bot_scripts:
                 del bot_scripts[script_key]
            return False
        except Exception as e:
            logger.error(f"Error checking process status for {script_key}: {e}", exc_info=True)
            return False
    return False


def kill_process_tree(process_info):
    """Kill a process and all its children, ensuring log file is closed."""
    pid = None
    log_file_closed = False
    script_key = process_info.get('script_key', 'N/A') 

    try:
        if 'log_file' in process_info and hasattr(process_info['log_file'], 'close') and not process_info['log_file'].closed:
            try:
                process_info['log_file'].close()
                log_file_closed = True
                logger.info(f"Closed log file for {script_key} (PID: {process_info.get('process', {}).get('pid', 'N/A')})")
            except Exception as log_e:
                logger.error(f"Error closing log file during kill for {script_key}: {log_e}")

        process = process_info.get('process')
        if process and hasattr(process, 'pid'):
           pid = process.pid
           if pid: 
                try:
                    parent = psutil.Process(pid)
                    children = parent.children(recursive=True)
                    logger.info(f"Attempting to kill process tree for {script_key} (PID: {pid}, Children: {[c.pid for c in children]})")

                    for child in children:
                        try:
                            child.terminate()
                            logger.info(f"Terminated child process {child.pid} for {script_key}")
                        except psutil.NoSuchProcess:
                            logger.warning(f"Child process {child.pid} for {script_key} already gone.")
                        except Exception as e:
                            logger.error(f"Error terminating child {child.pid} for {script_key}: {e}. Trying kill...")
                            try: child.kill(); logger.info(f"Killed child process {child.pid} for {script_key}")
                            except Exception as e2: logger.error(f"Failed to kill child {child.pid} for {script_key}: {e2}")

                    gone, alive = psutil.wait_procs(children, timeout=1)
                    for p in alive:
                        logger.warning(f"Child process {p.pid} for {script_key} still alive. Killing.")
                        try: p.kill()
                        except Exception as e: logger.error(f"Failed to kill child {p.pid} for {script_key} after wait: {e}")

                    try:
                        parent.terminate()
                        logger.info(f"Terminated parent process {pid} for {script_key}")
                        try: parent.wait(timeout=1)
                        except psutil.TimeoutExpired:
                            logger.warning(f"Parent process {pid} for {script_key} did not terminate. Killing.")
                            parent.kill()
                            logger.info(f"Killed parent process {pid} for {script_key}")
                    except psutil.NoSuchProcess:
                        logger.warning(f"Parent process {pid} for {script_key} already gone.")
                    except Exception as e:
                        logger.error(f"Error terminating parent {pid} for {script_key}: {e}. Trying kill...")
                        try: parent.kill(); logger.info(f"Killed parent process {pid} for {script_key}")
                        except Exception as e2: logger.error(f"Failed to kill parent {pid} for {script_key}: {e2}")

                except psutil.NoSuchProcess:
                    logger.warning(f"Process {pid or 'N/A'} for {script_key} not found during kill. Already terminated?")
           else: logger.error(f"Process PID is None for {script_key}.")
        elif log_file_closed: logger.warning(f"Process object missing for {script_key}, but log file closed.")
        else: logger.error(f"Process object missing for {script_key}, and no log file. Cannot kill.")
    except Exception as e:
        logger.error(f"❌ Unexpected error killing process tree for PID {pid or 'N/A'} ({script_key}): {e}", exc_info=True)

# --- Automatic Package Installation & Script Running ---

def attempt_install_pip(module_name, message):
    package_name = TELEGRAM_MODULES.get(module_name.lower(), module_name) 
    if package_name is None: 
        logger.info(f"Module '{module_name}' is core. Skipping pip install.")
        return False 
    try:
        bot.reply_to(message, f"🐍 Module `{module_name}` not found. Installing `{package_name}`...", parse_mode='Markdown')
        command = [sys.executable, '-m', 'pip', 'install', package_name]
        logger.info(f"Running install: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, encoding='utf-8', errors='ignore')
        if result.returncode == 0:
            logger.info(f"Installed {package_name}. Output:\n{result.stdout}")
            bot.reply_to(message, f"✅ Package `{package_name}` (for `{module_name}`) installed.", parse_mode='Markdown')
            return True
        else:
            error_msg = f"❌ Failed to install `{package_name}` for `{module_name}`.\nLog:\n```\n{result.stderr or result.stdout}\n```"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
            bot.reply_to(message, error_msg, parse_mode='Markdown')
            return False
    except Exception as e:
        error_msg = f"❌ Error installing `{package_name}`: {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message, error_msg)
        return False

def attempt_install_npm(module_name, user_folder, message):
    try:
        bot.reply_to(message, f"🟠 Node package `{module_name}` not found. Installing locally...", parse_mode='Markdown')
        command = ['npm', 'install', module_name]
        logger.info(f"Running npm install: {' '.join(command)} in {user_folder}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, cwd=user_folder, encoding='utf-8', errors='ignore')
        if result.returncode == 0:
            logger.info(f"Installed {module_name}. Output:\n{result.stdout}")
            bot.reply_to(message, f"✅ Node package `{module_name}` installed locally.", parse_mode='Markdown')
            return True
        else:
            error_msg = f"❌ Failed to install Node package `{module_name}`.\nLog:\n```\n{result.stderr or result.stdout}\n```"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
            bot.reply_to(message, error_msg, parse_mode='Markdown')
            return False
    except FileNotFoundError:
         error_msg = "❌ Error: 'npm' not found. Ensure Node.js/npm are installed and in PATH."
         logger.error(error_msg)
         bot.reply_to(message, error_msg)
         return False
    except Exception as e:
        error_msg = f"❌ Error installing Node package `{module_name}`: {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message, error_msg)
        return False

def run_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    
    # 👇--- SECURITY CHECK START ---👇
    if db is not None:
        try:
            user_doc = col_files.find_one({'_id': script_owner_id}, {'files_data': 1})
            if user_doc:
                for f in user_doc.get('files_data', []):
                    if f.get('name') == file_name:
                        if f.get('status') == "pending":
                            bot.reply_to(message_obj_for_reply, "⛔ **Access Denied:** File pending Owner Approval.")
                            return
                        break
        except:
            pass
    # 👆--- SECURITY CHECK END ---👆


    max_attempts = 2
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ Failed to run '{file_name}' after {max_attempts} attempts. Check logs.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Attempt {attempt} to run Python script: {script_path} (Key: {script_key})")

    try:
        if not os.path.exists(script_path):
             bot.reply_to(message_obj_for_reply, f"❌ Error: Script not found!")
             remove_user_file_db(script_owner_id, file_name)
             return

        # 🟢 SMART PORT ASSIGNMENT (Magic Here)
        # Ek khali port dhoondo
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            free_port = s.getsockname()[1]
        
        # User script ke liye naya Environment banao
        user_env = os.environ.copy()
        user_env["PORT"] = str(free_port) # User script ko naya port diya
        logger.info(f"Assigning Port {free_port} to user script {file_name}")

        if attempt == 1:
            check_command = [sys.executable, script_path]
            check_proc = None
            try:
                # Env pass kiya gaya hai
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore', env=user_env)
                stdout, stderr = check_proc.communicate(timeout=5)
                return_code = check_proc.returncode
                
                if return_code != 0 and stderr:
                    match_py = re.search(r"ModuleNotFoundError: No module named '(.+?)'", stderr)
                    if match_py:
                        module_name = match_py.group(1).strip().strip("'\"")
                        if attempt_install_pip(module_name, message_obj_for_reply):
                            time.sleep(2)
                            threading.Thread(target=run_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                            return
                        else:
                            bot.reply_to(message_obj_for_reply, f"❌ Install failed. Cannot run '{file_name}'.")
                            return
            except subprocess.TimeoutExpired:
                if check_proc: check_proc.kill()
            except Exception as e:
                 logger.error(f"Error in Python pre-check: {e}")

        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        
        startupinfo = None
        if os.name == 'nt':
             startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
             startupinfo.wShowWindow = subprocess.SW_HIDE
        
        # 🟢 Env yahan bhi pass kiya
        process = subprocess.Popen(
            [sys.executable, script_path], cwd=user_folder, stdout=log_file, stderr=log_file,
            stdin=subprocess.PIPE, startupinfo=startupinfo,
            encoding='utf-8', errors='ignore', env=user_env 
        )
        
        bot_scripts[script_key] = {
            'process': process, 'log_file': log_file, 'file_name': file_name,
            'chat_id': message_obj_for_reply.chat.id, 
            'script_owner_id': script_owner_id, 
            'start_time': datetime.now(), 'user_folder': user_folder, 'type': 'py', 'script_key': script_key
        }
        bot.reply_to(message_obj_for_reply, f"✅ Python script started! (Port: {free_port})")

    except Exception as e:
        if 'log_file' in locals() and log_file and not log_file.closed: log_file.close()
        logger.error(f"Error starting Python script: {e}")
        bot.reply_to(message_obj_for_reply, f"Error: {e}")

def run_js_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    max_attempts = 2
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ Failed to run '{file_name}' after attempts.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    
    try:
        if not os.path.exists(script_path):
             bot.reply_to(message_obj_for_reply, f"❌ Script not found!")
             remove_user_file_db(script_owner_id, file_name)
             return

        # 🟢 SMART PORT ASSIGNMENT
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            free_port = s.getsockname()[1]
        
        user_env = os.environ.copy()
        user_env["PORT"] = str(free_port)
        logger.info(f"Assigning Port {free_port} to JS script {file_name}")

        if attempt == 1:
            check_command = ['node', script_path]
            check_proc = None
            try:
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore', env=user_env)
                stdout, stderr = check_proc.communicate(timeout=5)
                return_code = check_proc.returncode
                
                if return_code != 0 and stderr:
                    match_js = re.search(r"Cannot find module '(.+?)'", stderr)
                    if match_js:
                        module_name = match_js.group(1).strip()
                        if not module_name.startswith('.'):
                             if attempt_install_npm(module_name, user_folder, message_obj_for_reply):
                                 time.sleep(2)
                                 threading.Thread(target=run_js_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                                 return
            except subprocess.TimeoutExpired:
                if check_proc: check_proc.kill()
            except Exception as e: logger.error(f"JS Check Error: {e}")

        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        
        process = subprocess.Popen(
            ['node', script_path], cwd=user_folder, stdout=log_file, stderr=log_file,
            stdin=subprocess.PIPE, encoding='utf-8', errors='ignore', env=user_env
        )
        
        bot_scripts[script_key] = {
            'process': process, 'log_file': log_file, 'file_name': file_name,
            'chat_id': message_obj_for_reply.chat.id, 
            'script_owner_id': script_owner_id, 
            'start_time': datetime.now(), 'user_folder': user_folder, 'type': 'js', 'script_key': script_key
        }
        bot.reply_to(message_obj_for_reply, f"✅ JS script started! (Port: {free_port})")

    except Exception as e:
        if 'log_file' in locals() and log_file and not log_file.closed: log_file.close()
        logger.error(f"Error starting JS script: {e}")
        bot.reply_to(message_obj_for_reply, f"Error: {e}")

# --- Map Telegram import names to actual PyPI package names ---
TELEGRAM_MODULES = {
    # Main Bot Frameworks
    'telebot': 'pyTelegramBotAPI',
    'telegram': 'python-telegram-bot',
    'python_telegram_bot': 'python-telegram-bot',
    'aiogram': 'aiogram',
    'pyrogram': 'pyrogram',
    'telethon': 'telethon',
    'telethon.sync': 'telethon', # Handle specific imports
    'from telethon.sync import telegramclient': 'telethon', # Example

    # Additional Libraries (add more specific mappings if import name differs)
    'telepot': 'telepot',
    'pytg': 'pytg',
    'tgcrypto': 'tgcrypto',
    'telegram_upload': 'telegram-upload',
    'telegram_send': 'telegram-send',
    'telegram_text': 'telegram-text',

    # MTProto & Low-Level
    'mtproto': 'telegram-mtproto', # Example, check actual package name
    'tl': 'telethon',  # Part of Telethon, install 'telethon'

    # Utilities & Helpers (examples, verify package names)
    'telegram_utils': 'telegram-utils',
    'telegram_logger': 'telegram-logger',
    'telegram_handlers': 'python-telegram-handlers',

    # Database Integrations (examples)
    'telegram_redis': 'telegram-redis',
    'telegram_sqlalchemy': 'telegram-sqlalchemy',

    # Payment & E-commerce (examples)
    'telegram_payment': 'telegram-payment',
    'telegram_shop': 'telegram-shop-sdk',

    # Testing & Debugging (examples)
    'pytest_telegram': 'pytest-telegram',
    'telegram_debug': 'telegram-debug',

    # Scraping & Analytics (examples)
    'telegram_scraper': 'telegram-scraper',
    'telegram_analytics': 'telegram-analytics',

    # NLP & AI (examples)
    'telegram_nlp': 'telegram-nlp-toolkit',
    'telegram_ai': 'telegram-ai', # Assuming this exists

    # Web & API Integration (examples)
    'telegram_api': 'telegram-api-client',
    'telegram_web': 'telegram-web-integration',

    # Gaming & Interactive (examples)
    'telegram_games': 'telegram-games',
    'telegram_quiz': 'telegram-quiz-bot',

    # File & Media Handling (examples)
    'telegram_ffmpeg': 'telegram-ffmpeg',
    'telegram_media': 'telegram-media-utils',

    # Security & Encryption (examples)
    'telegram_2fa': 'telegram-twofa',
    'telegram_crypto': 'telegram-crypto-bot',

    # Localization & i18n (examples)
    'telegram_i18n': 'telegram-i18n',
    'telegram_translate': 'telegram-translate',

    # Common non-telegram examples
    'bs4': 'beautifulsoup4',
    'requests': 'requests',
    'pillow': 'Pillow', # Note the capitalization difference
    'cv2': 'opencv-python', # Common import name for OpenCV
    'yaml': 'PyYAML',
    'dotenv': 'python-dotenv',
    'dateutil': 'python-dateutil',
    'pandas': 'pandas',
    'numpy': 'numpy',
    'flask': 'Flask',
    'django': 'Django',
    'sqlalchemy': 'SQLAlchemy',
    'asyncio': None, # Core module, should not be installed
    'json': None,    # Core module
    'datetime': None,# Core module
    'os': None,      # Core module
    'sys': None,     # Core module
    're': None,      # Core module
    'time': None,    # Core module
    'math': None,    # Core module
    'random': None,  # Core module
    'logging': None, # Core module
    'threading': None,# Core module
    'subprocess':None,# Core module
    'zipfile':None,  # Core module
    'tempfile':None, # Core module
    'shutil':None,   # Core module
    'sqlite3':None,  # Core module
    'psutil': 'psutil',
    'atexit': None   # Core module

}
# --- End Automatic Package Installation & Script Running ---


# --- MongoDB Database Operations (FIXED) ---
# 👇 STEP 2: IS CODE KO 'save_user_file' KI JAGAH PASTE KARO 👇

# --- Pending Files Manager (Restart Safety System) ---
PENDING_JSON = "pending_data.json"

def save_pending_entry(f_hash, user_id, file_name):
    data = {}
    try:
        if os.path.exists(PENDING_JSON):
            with open(PENDING_JSON, 'r') as f: data = json.load(f)
    except: pass
    data[f_hash] = {'uid': user_id, 'name': file_name}
    with open(PENDING_JSON, 'w') as f: json.dump(data, f)

def get_pending_entry(f_hash):
    try:
        if os.path.exists(PENDING_JSON):
            with open(PENDING_JSON, 'r') as f: return json.load(f).get(f_hash)
    except: pass
    return None

def remove_pending_entry(f_hash):
    try:
        if os.path.exists(PENDING_JSON):
            with open(PENDING_JSON, 'r') as f: data = json.load(f)
            if f_hash in data:
                del data[f_hash]; 
                with open(PENDING_JSON, 'w') as f: json.dump(data, f)
    except: pass

# --- Helper Functions (Re-added here) ---
def get_short_hash(file_name):
    return hashlib.md5(file_name.encode()).hexdigest()[:10]

# 👇 STEP 1: IS CODE KO TOP SECTION ME PASTE KARO 👇

# --- 1. Universal Package Mapping (Smart Install) ---
TELEGRAM_MODULES = {
    'telegram': 'python-telegram-bot', # 🔥 Sabse Important
    'telebot': 'pyTelegramBotAPI',
    'aiogram': 'aiogram',
    'pyrogram': 'pyrogram',
    'telethon': 'telethon',
    'requests': 'requests',
    'flask': 'Flask',
    'pymongo': 'pymongo',
    'dns': 'dnspython',
    'bs4': 'beautifulsoup4',
    'cv2': 'opencv-python-headless',
    'PIL': 'Pillow',
    'moviepy': 'moviepy',
    'pydub': 'pydub',
    'yt_dlp': 'yt-dlp',
    'numpy': 'numpy',
    'pandas': 'pandas',
    'sklearn': 'scikit-learn',
    'playwright': 'playwright'
}

# --- 2. Helper Functions (Smart Finder) ---
def get_short_hash(file_name):
    return hashlib.md5(file_name.encode()).hexdigest()[:10]

# 👇 JAGAH 1: IS CODE KO 'save_user_file' KE THIK UPAR PASTE KARO 👇

# --- 🛠️ Helper: Force Load Data (The Fix) ---
def ensure_user_data_loaded(user_id):
    """Ye function ensure karega ki RAM khali na ho"""
    # Agar RAM mein data hai, to achi baat hai
    if user_id in user_files and user_files[user_id]:
        return True
    
    # Agar RAM khali hai, to Database se bhar do
    if db is not None:
        try:
            user_doc = col_files.find_one({'_id': int(user_id)})
            if user_doc:
                loaded_files = []
                for f in user_doc.get('files_data', []):
                    loaded_files.append((f['name'], f['type']))
                
                # RAM Update
                user_files[user_id] = loaded_files
                return True
        except Exception as e:
            logger.error(f"Data Load Error: {e}")
    return False

# --- 1. Smart File Finder (Updated) ---
def get_file_name_from_hash(user_id, file_hash):
    user_id = int(user_id)
    
    # Step 1: Pehle Data Load Ensure karo (Yahi Magic hai)
    ensure_user_data_loaded(user_id)
    
    # Step 2: Ab RAM mein dhoondo
    if user_id in user_files:
        for item in user_files[user_id]:
            # Tuple vs String handle karo
            fname = item[0] if isinstance(item, (list, tuple)) else item
            if get_short_hash(fname) == file_hash:
                return fname
    return None


# --- 3. Save Function (With Pending Logic) ---
PENDING_JSON = "pending_data.json"

def save_pending_entry(f_hash, user_id, file_name):
    data = {}
    try:
        if os.path.exists(PENDING_JSON):
            with open(PENDING_JSON, 'r') as f: data = json.load(f)
    except: pass
    data[f_hash] = {'uid': user_id, 'name': file_name}
    with open(PENDING_JSON, 'w') as f: json.dump(data, f)

def get_pending_entry(f_hash):
    try:
        if os.path.exists(PENDING_JSON):
            with open(PENDING_JSON, 'r') as f: return json.load(f).get(f_hash)
    except: pass
    return None

def remove_pending_entry(f_hash):
    try:
        if os.path.exists(PENDING_JSON):
            with open(PENDING_JSON, 'r') as f: data = json.load(f)
            if f_hash in data:
                del data[f_hash]; 
                with open(PENDING_JSON, 'w') as f: json.dump(data, f)
    except: pass

# 👇 STEP 1: IS CODE KO 'save_user_file' KI JAGAH PASTE KARO 👇

def save_user_file(user_id, file_name, file_type='py', zip_content=None, zip_name=None, forced_risk=None):
    if user_id not in user_files: user_files[user_id] = []
    
    file_content = ""
    # Agar Zip nahi hai to content read karo
    if not zip_content:
        try:
            user_folder = get_user_folder(user_id)
            file_path = os.path.join(user_folder, file_name)
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f: file_content = f.read()
        except: pass

    # Risk Check Logic
    if forced_risk:
        # Agar ZIP function ne pehle hi risk dhoond liya hai
        is_risky, found_keyword = True, forced_risk
    else:
        # Normal File Scan
        scan_data = zip_content if zip_content else file_content
        # Note: Zip binary scan reliable nahi hota, isliye forced_risk zaroori hai
        is_risky, found_keyword = scan_content_for_risk(scan_data)
    
    file_status = "approved"
    if user_id != OWNER_ID and is_risky: file_status = "pending"

    user_files[user_id] = [f for f in user_files[user_id] if f[0] != file_name]
    user_files[user_id].append((file_name, file_type))

    if db is not None:
        try:
            new_entry = {
                "name": file_name, "type": file_type, "content": file_content,
                "zip_name": zip_name, "zip_content": zip_content,
                "status": file_status, "updated_at": datetime.now().isoformat()
            }
            col_files.update_one({'_id': user_id}, {'$pull': {'files_data': {'name': file_name}}}, upsert=True)
            col_files.update_one({'_id': user_id}, {'$push': {'files_data': new_entry}}, upsert=True)
        except Exception as e: logger.error(f"Mongo Save Error: {e}")

    # Agar Risky hai to Owner ko bhejo
    if file_status == "pending":
        f_hash = get_short_hash(file_name)
        save_pending_entry(f_hash, user_id, file_name) 
        
        msg = (f"⚠️ **SECURITY ALERT! (ZIP/File)**\n👤 User: `{user_id}`\n📂 File: `{file_name}`\n🚫 Found: `{found_keyword}`\nBot stopped this file.")
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("✅ Approve", callback_data=f"approve_{user_id}_{f_hash}"),
                   types.InlineKeyboardButton("❌ Delete", callback_data=f"disapprove_{user_id}_{f_hash}"))
        try:
            bot.send_message(OWNER_ID, msg, reply_markup=markup, parse_mode='Markdown')
            bot.send_message(user_id, f"⚠️ Your file `{file_name}` contains suspicious code (`{found_keyword}`). Sent for Owner Approval.")
        except: pass
        
        return True # Return True taki pata chale ki pending hai
    return False

def remove_user_file_db(user_id, file_name):
    # Update Memory
    if user_id in user_files:
        user_files[user_id] = [f for f in user_files[user_id] if f[0] != file_name]
        if not user_files[user_id]: del user_files[user_id]
        
        # Update MongoDB
        if db is not None:
            try:
                col_files.update_one({'_id': user_id}, {'$pull': {'files_data': {'name': file_name}}})
                logger.info(f"🗑️ Removed file '{file_name}' for {user_id} from Mongo")
            except Exception as e:
                logger.error(f"Error removing file from Mongo: {e}")

def add_active_user(user_id):
    if user_id not in active_users:
        active_users.add(user_id)
        if db is not None:
            try:
                col_config.update_one({'_id': 'global_data'}, {'$addToSet': {'active_users': user_id}}, upsert=True)
            except: pass

def save_subscription(user_id, expiry):
    user_subscriptions[user_id] = {'expiry': expiry}
    if db is not None:
        try:
            col_subs.update_one({'_id': user_id}, {'$set': {'expiry': expiry.isoformat()}}, upsert=True)
            logger.info(f"Saved subscription for {user_id} in Mongo")
        except Exception as e:
             logger.error(f"Error saving sub to Mongo: {e}")

def remove_subscription_db(user_id):
    if user_id in user_subscriptions:
        del user_subscriptions[user_id]
        if db is not None:
            try:
                col_subs.delete_one({'_id': user_id})
                logger.info(f"Removed subscription for {user_id} from Mongo")
            except: pass

def add_admin_db(admin_id):
    admin_ids.add(admin_id)
    if db is not None:
        try:
            col_config.update_one({'_id': 'global_data'}, {'$addToSet': {'admins': admin_id}}, upsert=True)
        except: pass

def remove_admin_db(admin_id):
    if admin_id == OWNER_ID: return False
    if admin_id in admin_ids:
        admin_ids.discard(admin_id)
        if db is not None:
            try:
                col_config.update_one({'_id': 'global_data'}, {'$pull': {'admins': admin_id}})
            except: pass
        return True
    return False
# --- End Database Operations ---

# --- Menu creation (Inline and ReplyKeyboards) ---
def create_main_menu_inline(user_id):
    markup = types.InlineKeyboardMarkup(row_width=2)
    # ... (Purane buttons wahi rahenge) ...
    
    buttons = [
        types.InlineKeyboardButton('📢 Updates Channel', url=UPDATE_CHANNEL),
        types.InlineKeyboardButton('📤 Upload File', callback_data='upload'),
        types.InlineKeyboardButton('📂 Check Files', callback_data='check_files'),
        types.InlineKeyboardButton('⚡ Bot Speed', callback_data='speed'),
        types.InlineKeyboardButton('📞 Contact Owner', url=f'https://t.me/{YOUR_USERNAME}')
    ]
    
    # Owner Special Section
    if user_id == OWNER_ID:
        markup.add(buttons[0])
        markup.add(buttons[1], buttons[2])
        markup.add(buttons[3])
        # 👇 NEW BUTTON
        markup.add(types.InlineKeyboardButton('🗃️ User Files (Admin)', callback_data='owner_all_users'))
        markup.add(buttons[4])
    else:
        markup.add(buttons[0])
        markup.add(buttons[1], buttons[2])
        markup.add(buttons[3], buttons[4])
        
    return markup

def create_reply_keyboard_main_menu(user_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    layout_to_use = ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC if user_id in admin_ids else COMMAND_BUTTONS_LAYOUT_USER_SPEC
    for row_buttons_text in layout_to_use:
        markup.add(*[types.KeyboardButton(text) for text in row_buttons_text])
    return markup

def create_control_buttons(script_owner_id, file_name, is_running=True): # Parameter renamed
    markup = types.InlineKeyboardMarkup(row_width=2)
    # Callbacks use script_owner_id
    if is_running:
        markup.row(
            types.InlineKeyboardButton("🔴 Stop", callback_data=f'stop_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("🔄 Restart", callback_data=f'restart_{script_owner_id}_{file_name}')
        )
        markup.row(
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("📜 Logs", callback_data=f'logs_{script_owner_id}_{file_name}')
        )
    else:
        markup.row(
            types.InlineKeyboardButton("🟢 Start", callback_data=f'start_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{file_name}')
        )
        markup.row(
            types.InlineKeyboardButton("📜 View Logs", callback_data=f'logs_{script_owner_id}_{file_name}')
        )
    markup.add(types.InlineKeyboardButton("🔙 Back to Files", callback_data='check_files'))
    return markup

def create_admin_panel():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('➕ Add Admin', callback_data='add_admin'),
        types.InlineKeyboardButton('➖ Remove Admin', callback_data='remove_admin')
    )
    markup.row(types.InlineKeyboardButton('📋 List Admins', callback_data='list_admins'))
    markup.row(types.InlineKeyboardButton('🔙 Back to Main', callback_data='back_to_main'))
    return markup

def create_subscription_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('➕ Add Subscription', callback_data='add_subscription'),
        types.InlineKeyboardButton('➖ Remove Subscription', callback_data='remove_subscription')
    )
    markup.row(types.InlineKeyboardButton('🔍 Check Subscription', callback_data='check_subscription'))
    markup.row(types.InlineKeyboardButton('🔙 Back to Main', callback_data='back_to_main'))
    return markup
# --- End Menu Creation ---

# --- File Handling ---
# 👇 STEP 2: IS CODE KO 'handle_zip_file' KI JAGAH PASTE KARO 👇

def handle_zip_file(downloaded_file_content, file_name_zip, message):
    user_id = message.from_user.id
    user_folder = get_user_folder(user_id)
    temp_dir = None 
    try:
        # 1. Zip ko Temp folder me extract karo (Checking ke liye)
        temp_dir = tempfile.mkdtemp(prefix=f"user_{user_id}_zip_check_")
        zip_path = os.path.join(temp_dir, file_name_zip)
        with open(zip_path, 'wb') as new_file: new_file.write(downloaded_file_content)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        # 2. 🛡️ DEEP SCANNING (Har folder aur file ko check karo)
        risky_found = False
        risky_reason = None
        
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                # Sirf code files ko scan karo
                if file.endswith(('.py', '.js', '.sh', '.txt', '.json')): 
                    file_check_path = os.path.join(root, file)
                    try:
                        with open(file_check_path, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read()
                            is_bad, keyword = scan_content_for_risk(content)
                            if is_bad:
                                risky_found = True
                                risky_reason = f"{keyword} (found in {file})"
                                break
                    except: pass
            if risky_found: break

        # 3. Main Script Dhoondo
        user_files_list = []
        for root, dirs, files in os.walk(temp_dir):
            for f in files: user_files_list.append(f)
            
        main_script_name = None; file_type = None
        preferred_py = ['main.py', 'bot.py', 'app.py']; preferred_js = ['index.js', 'main.js', 'bot.js']
        
        for p in preferred_py:
            if p in user_files_list: main_script_name = p; file_type = 'py'; break
        if not main_script_name:
             for p in preferred_js:
                 if p in user_files_list: main_script_name = p; file_type = 'js'; break
        if not main_script_name:
            # Fallback: Pehli .py ya .js file utha lo
            py_files = [f for f in user_files_list if f.endswith('.py')]
            js_files = [f for f in user_files_list if f.endswith('.js')]
            if py_files: main_script_name = py_files[0]; file_type = 'py'
            elif js_files: main_script_name = js_files[0]; file_type = 'js'
        
        if not main_script_name:
            bot.reply_to(message, "❌ No Python (.py) or Node (.js) script found inside ZIP!"); return

        # 4. Save to Database (Aur check karo ki approve chahiye ya nahi)
        is_pending = save_user_file(
            user_id, 
            main_script_name, 
            file_type, 
            zip_content=downloaded_file_content, 
            zip_name=file_name_zip,
            forced_risk=risky_reason # 🔥 Yahan humne bata diya ki risk mila hai
        )
        
        # 5. DECISION TIME
        if is_pending:
            # 🛑 Agar Pending hai, to YAHIN RUK JAO.
            # Files ko user folder mein move MAT karo.
            # User ko message `save_user_file` pehle hi bhej chuka hai.
            return 
        else:
            # ✅ SAFE: Ab User Folder me shift karo aur run karo
            
            # Zip ko user folder me save karo
            final_zip_path = os.path.join(user_folder, file_name_zip)
            with open(final_zip_path, 'wb') as f: f.write(downloaded_file_content)
            
            # Extract to real user folder
            with zipfile.ZipFile(final_zip_path, 'r') as z:
                z.extractall(user_folder)

            # Requirements Install (Agar safe hai)
            req_file_path = os.path.join(user_folder, 'requirements.txt')
            if os.path.exists(req_file_path):
                bot.reply_to(message, "📦 Installing modules from ZIP...", parse_mode='Markdown')
                try: subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", req_file_path])
                except: pass

            bot.reply_to(message, f"✅ Zip Verified & Safe. Starting `{main_script_name}`...", parse_mode='Markdown')

            # Script Run
            main_script_path = os.path.join(user_folder, main_script_name)
            if file_type == 'py':
                 threading.Thread(target=run_script, args=(main_script_path, user_id, user_folder, main_script_name, message)).start()
            elif file_type == 'js':
                 threading.Thread(target=run_js_script, args=(main_script_path, user_id, user_folder, main_script_name, message)).start()

    except zipfile.BadZipFile:
        bot.reply_to(message, "❌ Error: Invalid ZIP file.")
    except Exception as e:
        logger.error(f"❌ Error processing zip for {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error: {str(e)}")
    finally:
        # Temp folder (Kachra) saaf karo
        if temp_dir and os.path.exists(temp_dir):
            try: shutil.rmtree(temp_dir)
            except: pass


def handle_js_file(file_path, script_owner_id, user_folder, file_name, message):
    try:
        save_user_file(script_owner_id, file_name, 'js')
        threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
    except Exception as e:
        logger.error(f"❌ Error processing JS file {file_name} for {script_owner_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error processing JS file: {str(e)}")

def handle_py_file(file_path, script_owner_id, user_folder, file_name, message):
    try:
        save_user_file(script_owner_id, file_name, 'py')
        threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
    except Exception as e:
        logger.error(f"❌ Error processing Python file {file_name} for {script_owner_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error processing Python file: {str(e)}")
# --- End File Handling ---


# --- Logic Functions (called by commands and text handlers) ---
def _logic_send_welcome(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # 👇 YE CHECK LAGAO 👇
    if not is_user_joined(user_id):
        bot.send_message(chat_id, "⚠️ **Please Join Our Channels First!**\n\nBot use karne ke liye neeche diye gaye channels join karein.", 
                         reply_markup=get_fsub_markup(), parse_mode='Markdown')
        return
    # 👆 YAHAN TAK 👆

    user_name = message.from_user.first_name
    user_username = message.from_user.username

    logger.info(f"Welcome request from user_id: {user_id}, username: @{user_username}")

    if bot_locked and user_id not in admin_ids:
        bot.send_message(chat_id, "⚠️ Bot locked by admin. Try later.")
        return

    user_bio = "Could not fetch bio"; photo_file_id = None
    try: user_bio = bot.get_chat(user_id).bio or "No bio"
    except Exception: pass
    try:
        user_profile_photos = bot.get_user_profile_photos(user_id, limit=1)
        if user_profile_photos.photos: photo_file_id = user_profile_photos.photos[0][-1].file_id
    except Exception: pass

    if user_id not in active_users:
        add_active_user(user_id)
        try:
            owner_notification = (f"🎉 New user!\n👤 Name: {user_name}\n✳️ User: @{user_username or 'N/A'}\n"
                                  f"🆔 ID: `{user_id}`\n📝 Bio: {user_bio}")
            bot.send_message(OWNER_ID, owner_notification, parse_mode='Markdown')
            if photo_file_id: bot.send_photo(OWNER_ID, photo_file_id, caption=f"Pic of new user {user_id}")
        except Exception as e: logger.error(f"⚠️ Failed to notify owner about new user {user_id}: {e}")

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
    expiry_info = ""
    if user_id == OWNER_ID: user_status = "👑 Owner"
    elif user_id in admin_ids: user_status = "🛡️ Admin"
    elif user_id in user_subscriptions:
        expiry_date = user_subscriptions[user_id].get('expiry')
        if expiry_date and expiry_date > datetime.now():
            user_status = "⭐ Premium"; days_left = (expiry_date - datetime.now()).days
            expiry_info = f"\n⏳ Subscription expires in: {days_left} days"
        else: user_status = "🆓 Free User (Expired Sub)"; remove_subscription_db(user_id) # Clean up expired
    else: user_status = "🆓 Free User"

    welcome_msg_text = (f"〽️ Welcome, {user_name}!\n\n🆔 Your User ID: `{user_id}`\n"
                        f"✳️ Username: `@{user_username or 'Not set'}`\n"
                        f"🔰 Your Status: {user_status}{expiry_info}\n"
                        f"📁 Files Uploaded: {current_files} / {limit_str}\n\n"
                        f"🤖 Host & run Python (`.py`) or JS (`.js`) scripts.\n"
                        f"   Upload single scripts or `.zip` archives.\n\n"
                        f"👇 Use buttons or type commands.")
    main_reply_markup = create_reply_keyboard_main_menu(user_id)
    try:
        if photo_file_id: bot.send_photo(chat_id, photo_file_id)
        bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error sending welcome to {user_id}: {e}", exc_info=True)
        try: bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup, parse_mode='Markdown') # Fallback without photo
        except Exception as fallback_e: logger.error(f"Fallback send_message failed for {user_id}: {fallback_e}")

def _logic_updates_channel(message):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('📢 Updates Channel', url=UPDATE_CHANNEL))
    bot.reply_to(message, "Visit our Updates Channel:", reply_markup=markup)

def _logic_upload_file(message):
    user_id = message.from_user.id
    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked by admin, cannot accept files.")
        return

    # Removed free_mode check, relies on get_user_file_limit and FREE_USER_LIMIT
    # Users need to be admin or subscribed to upload if FREE_USER_LIMIT is 0
    # For now, FREE_USER_LIMIT > 0, so free users can upload up to that limit.
    # If we want to restrict free users entirely, set FREE_USER_LIMIT to 0.
    # For this implementation, free users get FREE_USER_LIMIT.

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.reply_to(message, f"⚠️ File limit ({current_files}/{limit_str}) reached. Delete files first.")
        return
    bot.reply_to(message, "📤 Send your Python (`.py`), JS (`.js`), or ZIP (`.zip`) file.")

def _logic_check_files(message):
    user_id = message.from_user.id
    # Callback queries aur Message objects dono handle karega
    if isinstance(message, types.CallbackQuery):
        message = message.message
        
    chat_id = message.chat.id
    user_files_list = user_files.get(user_id, [])
    
    if not user_files_list:
        bot.reply_to(message, "📂 Your files:\n\n(No files uploaded yet)")
        return

    markup = types.InlineKeyboardMarkup(row_width=1)
    files_skipped = []

    for file_name, file_type in sorted(user_files_list):
        is_running = is_bot_running(user_id, file_name)
        status_icon = "🟢 Running" if is_running else "🔴 Stopped"
        btn_text = f"{file_name} ({file_type}) - {status_icon}"
        
        # Callback Data Banayein
        cb_data = f'file_{user_id}_{file_name}'
        
        # ✅ Check Length: Agar 64 se bada hai to button mat banao (Crash bachane ke liye)
        if len(cb_data.encode('utf-8')) > 64:
            files_skipped.append(file_name)
            continue
            
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=cb_data))
        
    markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
    
    msg_text = "📂 Your files:\nClick to manage."
    
    # Agar koi file skip hui hai to user ko batao
    if files_skipped:
        skipped_names = "\n".join([f"- `{n}`" for n in files_skipped])
        msg_text += f"\n\n⚠️ **Hidden Files (Name too long):**\n{skipped_names}\n\n*Solution:* Inhe delete karke chota naam rakhein."

    bot.reply_to(message, msg_text, reply_markup=markup, parse_mode='Markdown')

def _logic_bot_speed(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    start_time_ping = time.time()
    wait_msg = bot.reply_to(message, "🏃 Testing speed...")
    try:
        bot.send_chat_action(chat_id, 'typing')
        response_time = round((time.time() - start_time_ping) * 1000, 2)
        status = "🔓 Unlocked" if not bot_locked else "🔒 Locked"
        # mode = "💰 Free Mode: ON" if free_mode else "💸 Free Mode: OFF" # Removed free_mode
        if user_id == OWNER_ID: user_level = "👑 Owner"
        elif user_id in admin_ids: user_level = "🛡️ Admin"
        elif user_id in user_subscriptions and user_subscriptions[user_id].get('expiry', datetime.min) > datetime.now(): user_level = "⭐ Premium"
        else: user_level = "🆓 Free User"
        speed_msg = (f"⚡ Bot Speed & Status:\n\n⏱️ API Response Time: {response_time} ms\n"
                     f"🚦 Bot Status: {status}\n"
                     # f"模式 Mode: {mode}\n" # Removed
                     f"👤 Your Level: {user_level}")
        bot.edit_message_text(speed_msg, chat_id, wait_msg.message_id)
    except Exception as e:
        logger.error(f"Error during speed test (cmd): {e}", exc_info=True)
        bot.edit_message_text("❌ Error during speed test.", chat_id, wait_msg.message_id)

def _logic_contact_owner(message):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('📞 Contact Owner', url=f'https://t.me/{YOUR_USERNAME.replace("@", "")}'))
    bot.reply_to(message, "Click to contact Owner:", reply_markup=markup)

# --- Admin Logic Functions ---
def _logic_subscriptions_panel(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    bot.reply_to(message, "💳 Subscription Management\nUse inline buttons from /start or admin command menu.", reply_markup=create_subscription_menu())

def _logic_statistics(message):
    # No admin check here, allow all users but show admin-specific info if admin
    user_id = message.from_user.id
    total_users = len(active_users)
    total_files_records = sum(len(files) for files in user_files.values())

    running_bots_count = 0
    user_running_bots = 0

    for script_key_iter, script_info_iter in list(bot_scripts.items()):
        s_owner_id, _ = script_key_iter.split('_', 1) # Extract owner_id from key
        if is_bot_running(int(s_owner_id), script_info_iter['file_name']):
            running_bots_count += 1
            if int(s_owner_id) == user_id:
                user_running_bots +=1

    stats_msg_base = (f"📊 Bot Statistics:\n\n"
                      f"👥 Total Users: {total_users}\n"
                      f"📂 Total File Records: {total_files_records}\n"
                      f"🟢 Total Active Bots: {running_bots_count}\n")

    if user_id in admin_ids:
        stats_msg_admin = (f"🔒 Bot Status: {'🔴 Locked' if bot_locked else '🟢 Unlocked'}\n"
                           # f"💰 Free Mode: {'✅ ON' if free_mode else '❌ OFF'}\n" # Removed
                           f"🤖 Your Running Bots: {user_running_bots}")
        stats_msg = stats_msg_base + stats_msg_admin
    else:
        stats_msg = stats_msg_base + f"🤖 Your Running Bots: {user_running_bots}"

    bot.reply_to(message, stats_msg)


# 👇 BROADCAST LOGIC START 👇

def execute_broadcast(msg_text, photo, video, document, admin_chat_id):
    count = 0
    blocked = 0
    users_list = list(active_users) # Active users ki list lo
    
    try: bot.send_message(admin_chat_id, f"⏳ Broadcast started to {len(users_list)} users...")
    except: pass

    for uid in users_list:
        try:
            # Media Support (Future proofing) or Text
            if photo: bot.send_photo(uid, photo, caption=msg_text)
            elif video: bot.send_video(uid, video, caption=msg_text)
            elif document: bot.send_document(uid, document, caption=msg_text)
            else: bot.send_message(uid, msg_text)
            count += 1
        except Exception as e:
            blocked += 1
        time.sleep(0.05) # Spam rokne ke liye delay
    
    try: bot.send_message(admin_chat_id, f"✅ Broadcast Complete.\n\n📨 Sent: {count}\n🚫 Blocked/Failed: {blocked}")
    except: pass

def process_broadcast_message(message):
    if message.text == '/cancel':
        bot.reply_to(message, "❌ Broadcast cancelled.")
        return

    msg_text = message.text
    # Simple logic for now (Text based, as per your button logic)
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("✅ Confirm Send", callback_data="confirm_broadcast_"),
               types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_broadcast"))
    
    preview = f"📢 **Broadcast Preview**\n\n{msg_text}" if msg_text else "📢 **Broadcast Preview**\n(Media)"
    bot.reply_to(message, preview, reply_markup=markup, parse_mode='Markdown')

def _logic_broadcast_init(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    msg = bot.reply_to(message, "📢 **Broadcast Mode**\n\nSend the message you want to broadcast to all users.\nType /cancel to abort.", parse_mode='Markdown')
    bot.register_next_step_handler(msg, process_broadcast_message)

# 👆 BROADCAST LOGIC END 👆

def _logic_toggle_lock_bot(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    global bot_locked
    bot_locked = not bot_locked
    status = "locked" if bot_locked else "unlocked"
    logger.warning(f"Bot {status} by Admin {message.from_user.id} via command/button.")
    bot.reply_to(message, f"🔒 Bot has been {status}.")

# def _logic_toggle_free_mode(message): # Removed
#     pass

def _logic_admin_panel(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    bot.reply_to(message, "👑 Admin Panel\nManage admins. Use inline buttons from /start or admin menu.",
                 reply_markup=create_admin_panel())

def _logic_run_all_scripts(message_or_call):
    if isinstance(message_or_call, telebot.types.Message):
        admin_user_id = message_or_call.from_user.id
        admin_chat_id = message_or_call.chat.id
        reply_func = lambda text, **kwargs: bot.reply_to(message_or_call, text, **kwargs)
        admin_message_obj_for_script_runner = message_or_call
    elif isinstance(message_or_call, telebot.types.CallbackQuery):
        admin_user_id = message_or_call.from_user.id
        admin_chat_id = message_or_call.message.chat.id
        bot.answer_callback_query(message_or_call.id)
        reply_func = lambda text, **kwargs: bot.send_message(admin_chat_id, text, **kwargs)
        admin_message_obj_for_script_runner = message_or_call.message 
    else:
        logger.error("Invalid argument for _logic_run_all_scripts")
        return

    if admin_user_id not in admin_ids:
        reply_func("⚠️ Admin permissions required.")
        return

    reply_func("⏳ Starting process to run all user scripts. This may take a while...")
    logger.info(f"Admin {admin_user_id} initiated 'run all scripts' from chat {admin_chat_id}.")

    started_count = 0; attempted_users = 0; skipped_files = 0; error_files_details = []

    # Use a copy of user_files keys and values to avoid modification issues during iteration
    all_user_files_snapshot = dict(user_files)

    for target_user_id, files_for_user in all_user_files_snapshot.items():
        if not files_for_user: continue
        attempted_users += 1
        logger.info(f"Processing scripts for user {target_user_id}...")
        user_folder = get_user_folder(target_user_id)

        for file_name, file_type in files_for_user:
            # script_owner_id for key context is target_user_id
            if not is_bot_running(target_user_id, file_name):
                file_path = os.path.join(user_folder, file_name)
                if os.path.exists(file_path):
                    logger.info(f"Admin {admin_user_id} attempting to start '{file_name}' ({file_type}) for user {target_user_id}.")
                    try:
                        if file_type == 'py':
                            threading.Thread(target=run_script, args=(file_path, target_user_id, user_folder, file_name, admin_message_obj_for_script_runner)).start()
                            started_count += 1
                        elif file_type == 'js':
                            threading.Thread(target=run_js_script, args=(file_path, target_user_id, user_folder, file_name, admin_message_obj_for_script_runner)).start()
                            started_count += 1
                        else:
                            logger.warning(f"Unknown file type '{file_type}' for {file_name} (user {target_user_id}). Skipping.")
                            error_files_details.append(f"`{file_name}` (User {target_user_id}) - Unknown type")
                            skipped_files += 1
                        time.sleep(0.7) # Increased delay slightly
                    except Exception as e:
                        logger.error(f"Error queueing start for '{file_name}' (user {target_user_id}): {e}")
                        error_files_details.append(f"`{file_name}` (User {target_user_id}) - Start error")
                        skipped_files += 1
                else:
                    logger.warning(f"File '{file_name}' for user {target_user_id} not found at '{file_path}'. Skipping.")
                    error_files_details.append(f"`{file_name}` (User {target_user_id}) - File not found")
                    skipped_files += 1
            # else: logger.info(f"Script '{file_name}' for user {target_user_id} already running.")

    summary_msg = (f"✅ All Users' Scripts - Processing Complete:\n\n"
                   f"▶️ Attempted to start: {started_count} scripts.\n"
                   f"👥 Users processed: {attempted_users}.\n")
    if skipped_files > 0:
        summary_msg += f"⚠️ Skipped/Error files: {skipped_files}\n"
        if error_files_details:
             summary_msg += "Details (first 5):\n" + "\n".join([f"  - {err}" for err in error_files_details[:5]])
             if len(error_files_details) > 5: summary_msg += "\n  ... and more (check logs)."

    reply_func(summary_msg, parse_mode='Markdown')
    logger.info(f"Run all scripts finished. Admin: {admin_user_id}. Started: {started_count}. Skipped/Errors: {skipped_files}")


# --- Command Handlers & Text Handlers for ReplyKeyboard ---
@bot.message_handler(commands=['start', 'help'])
def command_send_welcome(message): _logic_send_welcome(message)

@bot.message_handler(commands=['status']) # Kept for direct command
def command_show_status(message): _logic_statistics(message) # Changed to call _logic_statistics


# 👇 Yahan Paste karein 👇

def _logic_admin_panel(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    bot.reply_to(message, "👑 Admin Panel\nManage admins.", reply_markup=create_admin_panel())

# ✅ Ye naya logic hai Admin Files dekhne ke liye
def _logic_owner_files(message):
    if message.from_user.id != OWNER_ID:
        bot.reply_to(message, "⚠️ Only Owner can access this.")
        return
    if not user_files:
        bot.reply_to(message, "📂 No users have uploaded files yet.")
        return
    markup = types.InlineKeyboardMarkup(row_width=1)
    for uid in user_files.keys():
        count = len(user_files[uid])
        markup.add(types.InlineKeyboardButton(f"👤 User: {uid} ({count} files)", callback_data=f"owner_view_{uid}"))
    markup.add(types.InlineKeyboardButton("🔙 Back (Hide)", callback_data='ignore'))
    bot.reply_to(message, "🗃️ **All User Files (Admin View):**", reply_markup=markup, parse_mode='Markdown')


# ✅ Yahan se naya Help & Guide function
def _logic_help_command(message):
    help_text = (
        "🤖 *How to Host Your Bot?*\n\n"
        "1️⃣ *Upload File:* Send your `.py` or `.js` file (or `.zip`).\n"
        "2️⃣ *Port Handling (IMPORTANT):*\n"
        "   Your code MUST use the `PORT` environment variable.\n\n"
        "🐍 *For Python (Flask/Telebot):*\n"
        "```python\n"
        "import os\n"
        "port = int(os.environ.get(\"PORT\", 8080))\n"
        "app.run(host='0.0.0.0', port=port)\n"
        "```\n\n"
        "🟨 *For Node.js:*\n"
        "```js\n"
        "const port = process.env.PORT || 8080;\n"
        "app.listen(port, () => console.log(`Running on ${port}`));\n"
        "```\n"
    )
    bot.reply_to(message, help_text, parse_mode='Markdown')


# ✅ Button mapping (agar already hai to sirf ensure karo ke ye line ho)
BUTTON_TEXT_TO_LOGIC = {
    "📢 Updates Channel": _logic_updates_channel,
    "📤 Upload File": _logic_upload_file,
    "📂 Check Files": _logic_check_files,
    "⚡ Bot Speed": _logic_bot_speed,
    "📞 Contact Owner": _logic_contact_owner,
    "📊 Statistics": _logic_statistics,
    "💳 Subscriptions": _logic_subscriptions_panel,
    "📢 Broadcast": _logic_broadcast_init,
    "🔒 Lock Bot": _logic_toggle_lock_bot,
    "🟢 Running All Code": lambda m: _logic_run_all_scripts(m),
    "👑 Admin Panel": _logic_admin_panel,
    "🗃️ User Files (Admin)": _logic_owner_files,
    "ℹ️ Help & Guide": _logic_help_command,   # 👈 Ye line zaroor rahe
}

@bot.message_handler(func=lambda message: message.text in BUTTON_TEXT_TO_LOGIC)
def handle_button_text(message):
    # 👇 FORCE SUB CHECK (Yahan Paste Karein) 👇
    if not is_user_joined(message.from_user.id):
        bot.reply_to(message, "⛔ **Access Denied!**\nPehle Channels join karein fir buttons use karein.", reply_markup=get_fsub_markup())
        return
    # 👆 YAHAN TAK 👆

    logic_func = BUTTON_TEXT_TO_LOGIC.get(message.text)
    if logic_func: logic_func(message)
    else: logger.warning(f"Button text '{message.text}' matched but no logic func.")

@bot.message_handler(commands=['updateschannel'])
def command_updates_channel(message): _logic_updates_channel(message)
@bot.message_handler(commands=['uploadfile'])
def command_upload_file(message): _logic_upload_file(message)
@bot.message_handler(commands=['checkfiles'])
def command_check_files(message): _logic_check_files(message)
@bot.message_handler(commands=['botspeed'])
def command_bot_speed(message): _logic_bot_speed(message)
@bot.message_handler(commands=['contactowner'])
def command_contact_owner(message): _logic_contact_owner(message)
@bot.message_handler(commands=['subscriptions'])
def command_subscriptions(message): _logic_subscriptions_panel(message)
@bot.message_handler(commands=['statistics']) # Alias for /status
def command_statistics(message): _logic_statistics(message)
@bot.message_handler(commands=['broadcast'])
def command_broadcast(message): _logic_broadcast_init(message)
@bot.message_handler(commands=['lockbot']) 
def command_lock_bot(message): _logic_toggle_lock_bot(message)
# @bot.message_handler(commands=['freemode']) # Removed
# def command_free_mode(message): _logic_toggle_free_mode(message)
@bot.message_handler(commands=['adminpanel'])
def command_admin_panel(message): _logic_admin_panel(message)
@bot.message_handler(commands=['runningallcode']) # Added
def command_run_all_code(message): _logic_run_all_scripts(message)


@bot.message_handler(commands=['ping'])
def ping(message):
    start_ping_time = time.time() 
    msg = bot.reply_to(message, "Pong!")
    latency = round((time.time() - start_ping_time) * 1000, 2)
    bot.edit_message_text(f"Pong! Latency: {latency} ms", message.chat.id, msg.message_id)


# --- Document (File) Handler ---
@bot.message_handler(content_types=['document'])
def handle_file_upload_doc(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # 👇 FORCE SUB CHECK (Yahan Paste Karein) 👇
    if not is_user_joined(user_id):
        bot.reply_to(message, "⛔ **Access Denied!**\nFile upload karne ke liye Channels join karein.", reply_markup=get_fsub_markup())
        return
    # 👆 YAHAN TAK 👆

    doc = message.document
    logger.info(f"Doc from {user_id}: {doc.file_name} ({doc.mime_type}), Size: {doc.file_size}")

    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked, cannot accept files.")
        return

    # File limit check (relies on FREE_USER_LIMIT being > 0 for free users)
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.reply_to(message, f"⚠️ File limit ({current_files}/{limit_str}) reached. Delete files via /checkfiles.")
        return

    file_name = doc.file_name
    if not file_name: bot.reply_to(message, "⚠️ No file name. Ensure file has a name."); return
    file_ext = os.path.splitext(file_name)[1].lower()
    if file_ext not in ['.py', '.js', '.zip']:
        bot.reply_to(message, "⚠️ Unsupported type! Only `.py`, `.js`, `.zip` allowed.")
        return
    max_file_size = 20 * 1024 * 1024 # 20 MB
    if doc.file_size > max_file_size:
        bot.reply_to(message, f"⚠️ File too large (Max: {max_file_size // 1024 // 1024} MB)."); return

    try:
        try:
            bot.forward_message(OWNER_ID, chat_id, message.message_id)
            bot.send_message(OWNER_ID, f"⬆️ File '{file_name}' from {message.from_user.first_name} (`{user_id}`)", parse_mode='Markdown')
        except Exception as e: logger.error(f"Failed to forward uploaded file to OWNER_ID {OWNER_ID}: {e}")

        download_wait_msg = bot.reply_to(message, f"⏳ Downloading `{file_name}`...")
        file_info_tg_doc = bot.get_file(doc.file_id)
        downloaded_file_content = bot.download_file(file_info_tg_doc.file_path)
        bot.edit_message_text(f"✅ Downloaded `{file_name}`. Processing...", chat_id, download_wait_msg.message_id)
        logger.info(f"Downloaded {file_name} for user {user_id}")
        user_folder = get_user_folder(user_id)

        # 👇 YAHAN GALTI THI, AB SAHI HAI 👇
        if file_ext == '.zip':
            handle_zip_file(downloaded_file_content, file_name, message)
        else:
            file_path = os.path.join(user_folder, file_name)
            with open(file_path, 'wb') as f: f.write(downloaded_file_content)
            logger.info(f"Saved single file to {file_path}")
            
            # Pass user_id as script_owner_id
            if file_ext == '.js': 
                handle_js_file(file_path, user_id, user_folder, file_name, message)
            elif file_ext == '.py': 
                handle_py_file(file_path, user_id, user_folder, file_name, message)
    
    # 👆 YAHAN TAK PASTE KARO 👆

    except telebot.apihelper.ApiTelegramException as e:
         logger.error(f"Telegram API Error handling file for {user_id}: {e}", exc_info=True)
         if "file is too big" in str(e).lower():
              bot.reply_to(message, f"❌ Telegram API Error: File too large to download (~20MB limit).")
         else: bot.reply_to(message, f"❌ Telegram API Error: {str(e)}. Try later.")
    except Exception as e:
        logger.error(f"❌ General error handling file for {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Unexpected error: {str(e)}")
# --- End Document Handler ---

# 👇 IS CODE KO PASTE KARO (Helper Function + Callback Handler) 👇
# 👇 STEP 2: IS COMPLETE CODE KO 'handle_callbacks' KI JAGAH PASTE KARO 👇

# --- Universal Auto-Installer & Runner ---
def run_script_via_approval(script_path, script_owner_id, user_folder, file_name, file_type, attempt=1):
    try:
        # 1. Notify User (Starting)
        if attempt == 1:
            try: bot.send_message(script_owner_id, f"🔄 **Starting Host:** `{file_name}`\n⚙️ Checking environment...", parse_mode='Markdown')
            except: pass

        # --- A. ZIP FILE HANDLING ---
        if file_name.endswith('.zip'):
            try:
                import zipfile
                with zipfile.ZipFile(script_path, 'r') as zip_ref:
                    zip_ref.extractall(user_folder)
                
                # Main Script Dhoondo
                extracted_files = os.listdir(user_folder)
                possible_mains = ['main.py', 'bot.py', 'app.py', 'index.js', 'main.js', 'bot.js']
                found_main = None
                
                for m in possible_mains:
                    if m in extracted_files: found_main = m; break
                
                if not found_main:
                    for f in extracted_files:
                        if f.endswith('.py') or f.endswith('.js'): found_main = f; break
                
                if found_main:
                    file_name = found_main
                    script_path = os.path.join(user_folder, found_main)
                    file_type = 'js' if found_main.endswith('.js') else 'py'
                    try: bot.send_message(script_owner_id, f"✅ Zip Extracted! Found main file: `{found_main}`")
                    except: pass
                else:
                    try: bot.send_message(script_owner_id, "❌ Error: Zip extracted but no executable (.py/.js) file found.")
                    except: pass
                    return

            except Exception as e:
                logger.error(f"Zip extraction failed: {e}")
                try: bot.send_message(script_owner_id, f"❌ Zip Extraction Error: {e}")
                except: pass
                return

        # --- B. AUTO-INSTALLER LOGIC ---
        if file_type == 'py':
            # Requirements.txt check
            req_path = os.path.join(user_folder, 'requirements.txt')
            if os.path.exists(req_path) and attempt == 1:
                try:
                    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", req_path])
                    try: bot.send_message(script_owner_id, "✅ Modules installed from requirements.txt")
                    except: pass
                except Exception as e:
                    logger.error(f"Req install failed: {e}")

            # Smart Auto-Fix for Missing Modules
            if attempt < 5:
                check_env = os.environ.copy(); check_env["PORT"] = "9999"
                check_cmd = [sys.executable, script_path]
                try:
                    proc = subprocess.Popen(check_cmd, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore', env=check_env)
                    try: stdout, stderr = proc.communicate(timeout=8)
                    except subprocess.TimeoutExpired: proc.kill(); stderr = ""

                    if proc.returncode != 0 and stderr:
                        match = re.search(r"No module named '(.+?)'", stderr)
                        if match:
                            missing_module = match.group(1)
                            pkg_name = TELEGRAM_MODULES.get(missing_module, missing_module)
                            
                            try: bot.send_message(script_owner_id, f"🛠️ **Installing missing module:** `{pkg_name}`...")
                            except: pass
                            
                            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg_name])
                            time.sleep(2)
                            
                            # Restart process with new module
                            run_script_via_approval(script_path, script_owner_id, user_folder, file_name, file_type, attempt=attempt+1)
                            return
                except: pass

        # --- C. REAL RUN (Port Assign) ---
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0)); free_port = s.getsockname()[1]
        
        user_env = os.environ.copy(); user_env["PORT"] = str(free_port)
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        
        cmd = [sys.executable, script_path] if file_type == 'py' else ['node', script_path]
        startupinfo = None
        if os.name == 'nt': startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        
        process = subprocess.Popen(cmd, cwd=user_folder, stdout=log_file, stderr=log_file, stdin=subprocess.PIPE, encoding='utf-8', errors='ignore', env=user_env, startupinfo=startupinfo)
        
        script_key = f"{script_owner_id}_{file_name}"
        bot_scripts[script_key] = {
            'process': process, 'log_file': log_file, 'file_name': file_name,
            'chat_id': script_owner_id, 'script_owner_id': script_owner_id, 
            'start_time': datetime.now(), 'user_folder': user_folder, 'type': file_type, 'script_key': script_key
        }

        # ⏳ CRASH CHECK
        time.sleep(3)
        if process.poll() is not None:
            if script_key in bot_scripts: del bot_scripts[script_key]
            if not log_file.closed: log_file.close()
            log_content = "No logs."
            try:
                with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f: log_content = f.read()
            except: pass
            if len(log_content) > 3000: log_content = log_content[-3000:] + "\n...(truncated)"
            
            try: bot.send_message(script_owner_id, f"❌ **Error in script for** `{file_name}`:\n\n```\n{log_content}\n```\n\nFix the script.", parse_mode='Markdown')
            except: pass
            return

        # ✅ SUCCESS
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("📂 Check Status", callback_data="check_files"))
        bot.send_message(script_owner_id, f"✅ **Hosting Started Successfully!** 🚀\n\n📄 File: `{file_name}`\n🔌 Port: `{free_port}`\n🟢 Status: Running", reply_markup=markup, parse_mode='Markdown')

    except Exception as e:
        if 'log_file' in locals() and log_file: log_file.close()
        logger.error(f"Run Error: {e}")


# --- Main Callback Handler (COMPLETE ALL COMMANDS) ---
@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call):
    user_id = call.from_user.id
    data = call.data
    
    # --- 0. Force Join Verify ---
    if data == 'verify_join':
        if is_user_joined(user_id):
            bot.answer_callback_query(call.id, "✅ Verified!")
            try: bot.delete_message(call.message.chat.id, call.message.message_id)
            except: pass
            _logic_send_welcome(call.message)
        else:
            bot.answer_callback_query(call.id, "❌ Join Channels First!", show_alert=True)
        return

    # --- 1. Approve & Auto-Run Logic ---
    if data.startswith('approve_'):
        if user_id != OWNER_ID: return 
        
        bot.answer_callback_query(call.id, "✅ Approving...")
        
        try:
            _, target_id_str, f_hash = data.split('_', 2)
            target_id = int(target_id_str)
            
            # JSON se naam nikalo
            pending_info = get_pending_entry(f_hash)
            fname = pending_info.get('name') if pending_info else get_file_name_from_hash(target_id, f_hash)
            
            if not fname:
                bot.answer_callback_query(call.id, "⚠️ File info expired.", show_alert=True); return

            user_folder = get_user_folder(target_id); file_path = os.path.join(user_folder, fname)
            
            # 🔥 FILE RESTORE Logic (Server Restart Protection)
            if not os.path.exists(file_path):
                if db is not None:
                    doc = col_files.find_one({'_id': target_id})
                    if doc:
                        for f in doc.get('files_data', []):
                            if f['name'] == fname:
                                # ZIP Restore
                                if f.get('zip_content') and f.get('zip_name'):
                                    try:
                                        import zipfile
                                        zip_p = os.path.join(user_folder, f['zip_name'])
                                        with open(zip_p, 'wb') as zf: zf.write(f['zip_content'])
                                        with zipfile.ZipFile(zip_p, 'r') as z: z.extractall(user_folder)
                                    except: pass
                                # Normal Restore
                                elif f.get('content'):
                                    with open(file_path, 'w', encoding='utf-8', errors='ignore') as fo: fo.write(f['content'])
                                break
            
            # DB Update
            if db is not None:
                col_files.update_one({'_id': target_id, 'files_data.name': fname}, {'$set': {'files_data.$.status': 'approved'}})
            
            try: bot.edit_message_text(f"✅ **Approved!**\nUser: `{target_id}`\nFile: `{fname}`\n🚀 Status: Starting...", call.message.chat.id, call.message.message_id, parse_mode='Markdown')
            except: pass
            
            try: bot.send_message(target_id, f"✅ **Approval Granted!**\nFile `{fname}` approved.", parse_mode='Markdown')
            except: pass
            
            remove_pending_entry(f_hash)
            
            ftype = 'js' if fname.endswith('.js') else 'py'
            threading.Thread(target=run_script_via_approval, args=(file_path, target_id, user_folder, fname, ftype)).start()

        except Exception as e:
            bot.send_message(OWNER_ID, f"⚠️ Approve Error: {e}")
            logger.error(f"Approve Error: {e}")
        return

    # --- 2. Reject Logic ---
    elif data.startswith('disapprove_'):
        if user_id != OWNER_ID: return 
        bot.answer_callback_query(call.id, "🗑️ Rejecting...")
        try:
            _, target_id_str, f_hash = data.split('_', 2)
            target_id = int(target_id_str)
            pending_info = get_pending_entry(f_hash)
            fname = pending_info.get('name') if pending_info else get_file_name_from_hash(target_id, f_hash)
            
            try: bot.edit_message_text(f"❌ **Rejected & Deleted**\nUser: `{target_id}`", call.message.chat.id, call.message.message_id, parse_mode='Markdown')
            except: pass

            if fname:
                try: bot.send_message(target_id, f"❌ **File Rejected!** 🔴\n📁 File: `{fname}`\n⚠️ Reason: Terms Violation.", parse_mode='Markdown')
                except: pass
                remove_user_file_db(target_id, fname)
                f_path = os.path.join(get_user_folder(target_id), fname)
                if os.path.exists(f_path): os.remove(f_path)
            remove_pending_entry(f_hash)
        except: pass
        return

    # --- 3. Owner Manager ---
    elif data == 'owner_all_users':
        if user_id != OWNER_ID: return
        if not user_files: bot.answer_callback_query(call.id, "No users.", show_alert=True); return
        markup = types.InlineKeyboardMarkup(row_width=1)
        for uid in user_files.keys(): markup.add(types.InlineKeyboardButton(f"👤 User: {uid} ({len(user_files[uid])})", callback_data=f"owner_view_{uid}"))
        markup.add(types.InlineKeyboardButton("🔙 Back", callback_data='back_to_main'))
        bot.edit_message_text("🗃️ **Select User:**", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')
        return
    
    elif data.startswith('owner_view_'):
        if user_id != OWNER_ID: return
        try:
            target_uid = int(data.split('_')[2])
            files = user_files.get(target_uid, [])
            markup = types.InlineKeyboardMarkup(row_width=1)
            for fname, ftype in files:
                is_running = is_bot_running(target_uid, fname)
                icon = "🟢" if is_running else "🔴"
                f_hash = get_short_hash(fname)
                markup.add(types.InlineKeyboardButton(f"{icon} {fname}", callback_data="ignore"), types.InlineKeyboardButton(f"🗑️ Del", callback_data=f"owner_del_{target_uid}_{f_hash}"))
            markup.add(types.InlineKeyboardButton("🔙 Back", callback_data='owner_all_users'))
            bot.edit_message_text(f"🗃️ **Managing:** `{target_uid}`", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')
        except: pass
        return

    elif data.startswith('owner_del_'):
        if user_id != OWNER_ID: return
        _, _, target_uid, f_hash = data.split('_', 3)
        target_uid = int(target_uid); fname = get_file_name_from_hash(target_uid, f_hash)
        if fname:
            if is_bot_running(target_uid, fname):
                key = f"{target_uid}_{fname}"; 
                if key in bot_scripts: kill_process_tree(bot_scripts[key]); del bot_scripts[key]
            remove_user_file_db(target_uid, fname)
            f_path = os.path.join(get_user_folder(target_uid), fname)
            if os.path.exists(f_path): os.remove(f_path)
            bot.answer_callback_query(call.id, "🗑️ Deleted!")
            # Refresh
            files = user_files.get(target_uid, [])
            markup = types.InlineKeyboardMarkup(row_width=1)
            for f, t in files:
                is_running = is_bot_running(target_uid, f)
                icon = "🟢" if is_running else "🔴"
                fh = get_short_hash(f)
                markup.add(types.InlineKeyboardButton(f"{icon} {f}", callback_data="ignore"), types.InlineKeyboardButton(f"🗑️ Del", callback_data=f"owner_del_{target_uid}_{fh}"))
            markup.add(types.InlineKeyboardButton("🔙 Back", callback_data='owner_all_users'))
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
        return

    # --- 4. General Routes ---
    elif data.startswith('file_'): file_control_callback(call)
    elif data == 'upload': upload_callback(call)
    elif data == 'check_files': check_files_callback(call)
    elif data == 'speed': speed_callback(call)
    elif data == 'back_to_main': back_to_main_callback(call)
    elif data == 'ignore': bot.answer_callback_query(call.id)
    elif data.startswith('start_'): start_bot_callback(call)
    elif data.startswith('stop_'): stop_bot_callback(call)
    elif data.startswith('restart_'): restart_bot_callback(call)
    elif data.startswith('delete_'): delete_bot_callback(call)
    elif data.startswith('logs_'): logs_bot_callback(call)
    
    # --- 5. Admin Panel Routes ---
    elif data == 'admin_panel': admin_required_callback(call, admin_panel_callback)
    elif data == 'add_admin': owner_required_callback(call, add_admin_init_callback) 
    elif data == 'remove_admin': owner_required_callback(call, remove_admin_init_callback) 
    elif data == 'list_admins': admin_required_callback(call, list_admins_callback)
    elif data == 'subscriptions': admin_required_callback(call, subscription_management_callback)
    elif data == 'add_subscription': admin_required_callback(call, add_subscription_init_callback)
    elif data == 'remove_subscription': admin_required_callback(call, remove_subscription_init_callback)
    elif data == 'check_subscription': check_subscription_init_callback(call)
    elif data == 'broadcast': admin_required_callback(call, broadcast_init_callback)
    elif data.startswith('confirm_broadcast_'): handle_confirm_broadcast(call)
    elif data == 'cancel_broadcast': handle_cancel_broadcast(call)
    elif data == 'lock_bot': admin_required_callback(call, lock_bot_callback)
    elif data == 'unlock_bot': admin_required_callback(call, unlock_bot_callback)
    elif data == 'running_all_code': admin_required_callback(call, run_all_scripts_callback)
    
    else: bot.answer_callback_query(call.id, "⚠️ Unknown Action")

def admin_required_callback(call, func_to_run):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    func_to_run(call) 

def owner_required_callback(call, func_to_run):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner permissions required.", show_alert=True)
        return
    func_to_run(call)

def upload_callback(call):
    user_id = call.from_user.id
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.answer_callback_query(call.id, f"⚠️ File limit ({current_files}/{limit_str}) reached.", show_alert=True)
        return
    bot.answer_callback_query(call.id) 
    bot.send_message(call.message.chat.id, "📤 Send your Python (`.py`), JS (`.js`), or ZIP (`.zip`) file.")

# --- Updated Button Functions (Hashes Logic) ---
def create_control_buttons(script_owner_id, file_name, is_running=True):
    markup = types.InlineKeyboardMarkup(row_width=2)
    f_hash = get_short_hash(file_name)
    if is_running:
        markup.row(types.InlineKeyboardButton("🔴 Stop", callback_data=f'stop_{script_owner_id}_{f_hash}'), types.InlineKeyboardButton("🔄 Restart", callback_data=f'restart_{script_owner_id}_{f_hash}'))
        markup.row(types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{f_hash}'), types.InlineKeyboardButton("📜 Logs", callback_data=f'logs_{script_owner_id}_{f_hash}'))
    else:
        markup.row(types.InlineKeyboardButton("🟢 Start", callback_data=f'start_{script_owner_id}_{f_hash}'), types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{f_hash}'))
        markup.row(types.InlineKeyboardButton("📜 View Logs", callback_data=f'logs_{script_owner_id}_{f_hash}'))
    markup.add(types.InlineKeyboardButton("🔙 Back to Files", callback_data='check_files'))
    return markup

# 👇 IS CODE KO 'check_files_callback' KI JAGAH PASTE KARO 👇

# --- 2. Check Files Button (Menu) ---
def check_files_callback(call):
    user_id = call.from_user.id
    
    # 🟢 Button dabate hi DB se taza data lo
    ensure_user_data_loaded(user_id)
    
    user_files_list = user_files.get(user_id, [])
    
    if not user_files_list:
        bot.answer_callback_query(call.id, "⚠️ No files found.")
        try:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
            bot.edit_message_text("📂 **Your Files:**\n\n(No files uploaded yet)", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')
        except: pass
        return

    bot.answer_callback_query(call.id)
    markup = types.InlineKeyboardMarkup(row_width=1)
    
    for item in sorted(user_files_list):
        fname = item[0] if isinstance(item, (list, tuple)) else item
        ftype = item[1] if isinstance(item, (list, tuple)) and len(item) > 1 else '?'
        
        is_running = is_bot_running(user_id, fname)
        status = "🟢" if is_running else "🔴"
        
        # Hash generate karo
        f_hash = get_short_hash(fname)
        markup.add(types.InlineKeyboardButton(f"{fname} ({ftype}) {status}", callback_data=f'file_{user_id}_{f_hash}'))
        
    markup.add(types.InlineKeyboardButton("🔙 Back", callback_data='back_to_main'))
    
    try:
        bot.edit_message_text("📂 **Your Files:**\nClick to manage.", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')
    except: pass

# --- 3. File Control (Click Action) ---
def file_control_callback(call):
    try:
        _, script_owner_id_str, f_hash = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        
        # 🟢 Yahan bhi Data Load ensure karo (Double Safety)
        ensure_user_data_loaded(script_owner_id)
        
        # File dhoondo
        file_name = get_file_name_from_hash(script_owner_id, f_hash)
        
        if not file_name: 
            # Agar ab bhi nahi mila, to list refresh kar do
            bot.answer_callback_query(call.id, "⚠️ Refreshing list...", show_alert=False)
            check_files_callback(call)
            return

        # Permission check
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ No Permission.", show_alert=True)
            return

        bot.answer_callback_query(call.id)
        is_running = is_bot_running(script_owner_id, file_name)
        status_text = '🟢 Running' if is_running else '🔴 Stopped'
        
        bot.edit_message_text(
            f"⚙️ **Controls:** `{file_name}`\nStatus: {status_text}", 
            call.message.chat.id, 
            call.message.message_id, 
            reply_markup=create_control_buttons(script_owner_id, file_name, is_running), 
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"File Control Error: {e}")


def start_bot_callback(call):
    try:
        _, script_owner_id_str, f_hash = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        file_name = get_file_name_from_hash(script_owner_id, f_hash)
        if not file_name: bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); check_files_callback(call); return
        if is_bot_running(script_owner_id, file_name): bot.answer_callback_query(call.id, "⚠️ Already running.", show_alert=True); return
        
        user_folder = get_user_folder(script_owner_id); file_path = os.path.join(user_folder, file_name)
        if not os.path.exists(file_path): bot.answer_callback_query(call.id, "⚠️ File Missing!", show_alert=True); return

        bot.answer_callback_query(call.id, f"⏳ Starting {file_name}...")
        user_files_list = user_files.get(script_owner_id, []); file_type = next((f[1] for f in user_files_list if f[0] == file_name), 'py')
        if file_type == 'py': threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        elif file_type == 'js': threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        time.sleep(1.5); is_running = is_bot_running(script_owner_id, file_name)
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_control_buttons(script_owner_id, file_name, is_running))   
    except Exception as e: logger.error(f"Error starting bot: {e}")

def stop_bot_callback(call):
    try:
        _, script_owner_id_str, f_hash = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        file_name = get_file_name_from_hash(script_owner_id, f_hash)
        if not file_name: bot.answer_callback_query(call.id, "File not found.", show_alert=True); return
        script_key = f"{script_owner_id}_{file_name}"
        if not is_bot_running(script_owner_id, file_name): bot.answer_callback_query(call.id, "Already stopped.", show_alert=True)
        else:
            bot.answer_callback_query(call.id, f"⏳ Stopping {file_name}...")
            process_info = bot_scripts.get(script_key)
            if process_info: kill_process_tree(process_info)
            if script_key in bot_scripts: del bot_scripts[script_key]
        time.sleep(0.5)
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_control_buttons(script_owner_id, file_name, False))
    except Exception as e: logger.error(f"Error stopping bot: {e}")

def restart_bot_callback(call):
    try:
        _, script_owner_id_str, f_hash = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        file_name = get_file_name_from_hash(script_owner_id, f_hash)
        if not file_name: bot.answer_callback_query(call.id, "File not found.", show_alert=True); return
        script_key = f"{script_owner_id}_{file_name}"
        if is_bot_running(script_owner_id, file_name):
            bot.answer_callback_query(call.id, "🔄 Restarting...")
            process_info = bot_scripts.get(script_key)
            if process_info: kill_process_tree(process_info)
            if script_key in bot_scripts: del bot_scripts[script_key]
            time.sleep(1)
        else: bot.answer_callback_query(call.id, "🔄 Starting...")
        user_folder = get_user_folder(script_owner_id); file_path = os.path.join(user_folder, file_name)
        user_files_list = user_files.get(script_owner_id, []); file_type = next((f[1] for f in user_files_list if f[0] == file_name), 'py')
        if file_type == 'py': threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        elif file_type == 'js': threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        time.sleep(1.5); is_running = is_bot_running(script_owner_id, file_name)
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_control_buttons(script_owner_id, file_name, is_running))   
    except Exception as e: logger.error(f"Error restarting bot: {e}")

def delete_bot_callback(call):
    try:
        _, script_owner_id_str, f_hash = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        file_name = get_file_name_from_hash(script_owner_id, f_hash)
        if not file_name: bot.answer_callback_query(call.id, "File not found.", show_alert=True); check_files_callback(call); return
        bot.answer_callback_query(call.id, f"🗑️ Deleting {file_name}...")
        script_key = f"{script_owner_id}_{file_name}"
        if is_bot_running(script_owner_id, file_name):
            process_info = bot_scripts.get(script_key)
            if process_info: kill_process_tree(process_info)
            if script_key in bot_scripts: del bot_scripts[script_key]
            time.sleep(0.5)
        user_folder = get_user_folder(script_owner_id); file_path = os.path.join(user_folder, file_name)
        if os.path.exists(file_path): os.remove(file_path)
        log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        if os.path.exists(log_path): os.remove(log_path)
        remove_user_file_db(script_owner_id, file_name)
        bot.edit_message_text(f"🗑️ Deleted `{file_name}`.", call.message.chat.id, call.message.message_id)
    except Exception as e: logger.error(f"Error deleting bot: {e}")

def logs_bot_callback(call):
    try:
        _, script_owner_id_str, f_hash = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        file_name = get_file_name_from_hash(script_owner_id, f_hash)
        if not file_name: bot.answer_callback_query(call.id, "File not found.", show_alert=True); return
        bot.answer_callback_query(call.id, "Fetching logs...")
        user_folder = get_user_folder(script_owner_id)
        log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_content = "(No logs found)"
        if os.path.exists(log_path):
             with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                 log_content = f.read()[-4000:]
                 if not log_content.strip(): log_content = "(Log file empty)"
        bot.send_message(call.message.chat.id, f"📜 Logs for `{file_name}`:\n```\n{log_content}\n```", parse_mode='Markdown')
    except Exception as e: logger.error(f"Error logs: {e}")

# --- Admin Callbacks & Others ---
def speed_callback(call):
    user_id = call.from_user.id; chat_id = call.message.chat.id; start = time.time()
    try:
        bot.edit_message_text("🏃 Testing...", chat_id, call.message.message_id)
        resp = round((time.time() - start) * 1000, 2); status = "🔒 Locked" if bot_locked else "🔓 Unlocked"
        bot.answer_callback_query(call.id)
        bot.edit_message_text(f"⚡ Speed: {resp}ms\nStatus: {status}", chat_id, call.message.message_id, reply_markup=create_main_menu_inline(user_id))
    except: pass

def back_to_main_callback(call):
    user_id = call.from_user.id; chat_id = call.message.chat.id
    try:
        bot.answer_callback_query(call.id)
        bot.edit_message_text(f"〽️ Welcome back!", chat_id, call.message.message_id, reply_markup=create_main_menu_inline(user_id))
    except: pass

# --- Simplified Admin Functions for brevity in paste ---
def subscription_management_callback(call): bot.answer_callback_query(call.id); bot.edit_message_text("💳 Subscriptions", call.message.chat.id, call.message.message_id, reply_markup=create_subscription_menu())
def lock_bot_callback(call): global bot_locked; bot_locked = True; bot.answer_callback_query(call.id, "🔒 Locked"); bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_main_menu_inline(call.from_user.id))
def unlock_bot_callback(call): global bot_locked; bot_locked = False; bot.answer_callback_query(call.id, "🔓 Unlocked"); bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_main_menu_inline(call.from_user.id))
def run_all_scripts_callback(call): _logic_run_all_scripts(call)
def broadcast_init_callback(call): bot.answer_callback_query(call.id); msg = bot.send_message(call.message.chat.id, "📢 Send message."); bot.register_next_step_handler(msg, process_broadcast_message)
def admin_panel_callback(call): bot.answer_callback_query(call.id); bot.edit_message_text("👑 Admin Panel", call.message.chat.id, call.message.message_id, reply_markup=create_admin_panel())
def add_admin_init_callback(call): bot.answer_callback_query(call.id); msg = bot.send_message(call.message.chat.id, "👑 Enter ID to promote."); bot.register_next_step_handler(msg, process_add_admin_id)
def remove_admin_init_callback(call): bot.answer_callback_query(call.id); msg = bot.send_message(call.message.chat.id, "👑 Enter ID to remove."); bot.register_next_step_handler(msg, process_remove_admin_id)
def list_admins_callback(call): bot.answer_callback_query(call.id); bot.edit_message_text(f"👑 Admins: {sorted(list(admin_ids))}", call.message.chat.id, call.message.message_id, reply_markup=create_admin_panel())
def add_subscription_init_callback(call): bot.answer_callback_query(call.id); msg = bot.send_message(call.message.chat.id, "💳 Enter 'ID days'."); bot.register_next_step_handler(msg, process_add_subscription_details)
def remove_subscription_init_callback(call): bot.answer_callback_query(call.id); msg = bot.send_message(call.message.chat.id, "💳 Enter ID to remove."); bot.register_next_step_handler(msg, process_remove_subscription_id)
def check_subscription_init_callback(call): bot.answer_callback_query(call.id); msg = bot.send_message(call.message.chat.id, "💳 Enter ID to check."); bot.register_next_step_handler(msg, process_check_subscription_id)
def handle_confirm_broadcast(call): 
    if call.from_user.id not in admin_ids: return
    threading.Thread(target=execute_broadcast, args=(call.message.reply_to_message.text, None, None, None, call.message.chat.id)).start(); bot.delete_message(call.message.chat.id, call.message.message_id)
def handle_cancel_broadcast(call): bot.delete_message(call.message.chat.id, call.message.message_id)

# --- End Callback Query Handlers ---

# --- Cleanup Function ---
def cleanup():
    logger.warning("Shutdown. Cleaning up processes...")
    script_keys_to_stop = list(bot_scripts.keys()) 
    if not script_keys_to_stop: logger.info("No scripts running. Exiting."); return
    logger.info(f"Stopping {len(script_keys_to_stop)} scripts...")
    for key in script_keys_to_stop:
        if key in bot_scripts: logger.info(f"Stopping: {key}"); kill_process_tree(bot_scripts[key])
        else: logger.info(f"Script {key} already removed.")
    logger.warning("Cleanup finished.")
atexit.register(cleanup)

# --- Auto Restart Function ---
def restart_program():
    """Restarts the current program automatically."""
    print("♻️ Critical Error! Restarting the bot in 5 seconds...")
    time.sleep(5)
    python = sys.executable
    os.execl(python, python, *sys.argv)

# --- Main Execution with Auto-Restart ---
if __name__ == '__main__':
    print("🤖 Bot Starting Up...")

    # 1. Server Start (Flask - Auto Port Fix)
    try:
        keep_alive()
        print("✅ Flask Server Started.")
    except Exception as e:
        print(f"⚠️ Flask Error: {e}")
        # Agar Server start nahi hua to Restart karo
        # restart_program() 

    # 2. Database Load
    try:
        load_data_from_mongo()
        print("✅ Data loaded from MongoDB successfully.")
    except Exception as e:
        print(f"⚠️ Database Error: {e}")
        # Database fail hua to Restart karo
        restart_program()
    
    # 3. Ultimate Bot Polling (Infinite Loop)
    print("🚀 Starting Bot Polling...")
    
    while True:
        try:
            # Ye line bot ko chalati hai
            bot.infinity_polling(timeout=60, long_polling_timeout=30)
        
        except Exception as e:
            # Error aate hi ye block chalega
            print(f"💥 Bot Crashed with Error: {e}")
            
            # Agar Error 409 (Conflict) hai, to thoda wait karo
            if "409" in str(e):
                print("⚠️ Conflict Error! (Shayad bot do jagah chal raha hai). Waiting 15s...")
                time.sleep(15)
            else:
                # Koi aur bada crash hai to SCRIPT KO RESTART KAR DO
                print("🔄 connection lost/Error... Restarting Script...")
                restart_program() 
            
        except KeyboardInterrupt:
            # Agar aapne manually band kiya
            print("🛑 Bot stopped by user.")
            sys.exit()
        except SystemExit:
            restart_program()
