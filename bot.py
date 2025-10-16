import os
import json
import time
import asyncio
import threading
import sys 
import traceback
from typing import Dict, List, Optional
from collections import defaultdict
from flask import Flask, jsonify

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.enums import ParseMode

from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from algoliasearch.search_client import SearchClient
from rapidfuzz import fuzz 

# ====================================================================
# ENVIRONMENT VARIABLES & CONFIGURATION
# ====================================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEB_SERVER_PORT = int(os.environ.get("PORT", 8080))
# Hardcoded Admin ID - Please ensure this is your correct Telegram User ID
ADMIN_IDS = [7263519581] 

DATABASE_URL = os.getenv("DATABASE_URL")

ALGOLIA_APP_ID = os.getenv("ALGOLIA_APPLICATION_ID")
ALGOLIA_SEARCH_KEY = os.getenv("ALGOLIA_SEARCH_KEY") 
ALGOLIA_INDEX_NAME = os.getenv("ALGOLIA_INDEX_NAME", "Media_index")

# 🚨 CORRECT LIBRARY CHANNEL ID
# NOTE: It is still best practice to set this in Render's environment variables.
CORRECT_LIBRARY_CHANNEL_ID = -1003138949015 

LIBRARY_CHANNEL_USERNAME = os.getenv("LIBRARY_CHANNEL_USERNAME", "MOVIEMAZA19")
# LIBRARY_CHANNEL_ID will now use the correct ID as default if not set in Env.
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", CORRECT_LIBRARY_CHANNEL_ID))
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "MOVIEMAZASU")
JOIN_GROUP_USERNAME = os.getenv("JOIN_GROUP_USERNAME", "THEGREATMOVIESL9")

# --- Diagnostic print of env presence (masks sensitive values) ---
print("Startup environment check:")
print(f" - BOT_TOKEN set: {'yes' if BOT_TOKEN else 'NO'}")
print(f" - DATABASE_URL set: {'yes' if DATABASE_URL else 'NO'}")
print(f" - ALGOLIA_APP_ID set: {'yes' if ALGOLIA_APP_ID else 'NO'}")
print(f" - ALGOLIA_SEARCH_KEY set: {'yes' if ALGOLIA_SEARCH_KEY else 'NO'}")
print(f" - PORT used by Flask: {WEB_SERVER_PORT}")

if not BOT_TOKEN:
    print("❌ ERROR: BOT_TOKEN is missing. The bot cannot authenticate with Telegram without a valid token.")
    print("Please set BOT_TOKEN in Render environment settings and redeploy.")
    sys.exit(1)

if not DATABASE_URL or not ALGOLIA_APP_ID or not ALGOLIA_SEARCH_KEY:
    print("⚠️  WARNING: Missing non-critical environment variables (DB/Algolia).")
    print("⚠️  Running with reduced functionality (indexing/search may fail).")
    # do NOT overwrite values with dummy placeholders -- fail fast only for BOT_TOKEN

Base = declarative_base()
engine = None
SessionLocal = None

class Movie(Base):
    __tablename__ = "movies"
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, index=True, nullable=False)
    post_id = Column(Integer, unique=True, nullable=False)
    
bot = None
algolia_index = None
DEMO_MODE = False

try:
    bot = Bot(token=BOT_TOKEN)
except Exception as e:
    print(f"⚠️  Could not initialize bot: {e}")
    print("❌ Bot initialization failed. Exiting so Render shows a failed deploy and you can fix env.")
    print(traceback.format_exc())
    sys.exit(1)

dp = Dispatcher()

# ====================================================================
# INITIALIZATION & DATABASE LOGIC
# ====================================================================

def initialize_db_and_algolia_with_retry(max_retries: int = 5, base_delay: float = 2.0) -> bool:
    """Initialize DB and Algolia with exponential backoff retry logic."""
    global engine, SessionLocal, algolia_index
    
    for attempt in range(max_retries):
        try:
            print(f"Attempting to initialize PostgreSQL and Algolia... (Attempt {attempt + 1}/{max_retries})")
            
            db_url = DATABASE_URL
            if db_url and db_url.startswith("postgresql://"):
                db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
            
            if db_url:
                engine = create_engine(
                    db_url,
                    pool_pre_ping=True,
                    pool_size=10,
                    max_overflow=20,
                    pool_timeout=30,
                    pool_recycle=3600,
                    connect_args={"connect_timeout": 10}
                )
                Base.metadata.create_all(bind=engine)
                SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
                
                test_session = SessionLocal()
                try:
                    test_session.execute(text("SELECT 1"))
                    test_session.close()
                    print("✅ PostgreSQL connection verified.")
                except Exception as e:
                    test_session.close()
                    raise Exception(f"DB health check failed: {e}")
            else:
                print("⚠️ DATABASE_URL not set — skipping DB initialization.")

            if ALGOLIA_APP_ID and ALGOLIA_SEARCH_KEY:
                algolia_client = SearchClient.create(ALGOLIA_APP_ID, ALGOLIA_SEARCH_KEY)
                algolia_index = algolia_client.init_index(ALGOLIA_INDEX_NAME)
                
                try:
                    algolia_index.search("test", {'hitsPerPage': 1})
                    print("✅ Algolia connection verified.")
                except Exception as e:
                    print(f"⚠️ Algolia health check warning: {e}")
            else:
                print("⚠️ ALGOLIA credentials missing — skipping Algolia initialization.")
            
            print("✅ PostgreSQL & Algolia Clients Initialized Successfully (if configured).")
            return True

        except Exception as e:
            print(f"❌ Initialization attempt {attempt + 1} failed: {e}")
            print(traceback.format_exc())
            
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                print(f"⏳ Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
            else:
                print("❌ CRITICAL: All initialization attempts failed.")
                print("⚠️ Bot will continue running with degraded functionality.")
                return False
    
    return False

if not initialize_db_and_algolia_with_retry():
    print("⚠️ WARNING: Database/Search initialization failed after all retries.")
    print("⚠️ Bot will run with limited functionality. Admin commands may not work.")

def get_db():
    """Database session dependency with error handling."""
    if not SessionLocal:
        print("⚠️ Database not initialized. Cannot provide session.")
        return
    
    db_session = SessionLocal()
    try:
        yield db_session
    finally:
        db_session.close()

# (rest of file kept identical) ...
# ====================================================================
# BOT STATE & SEARCH UTILITIES
# ====================================================================

user_sessions: Dict[int, Dict] = defaultdict(dict)
verified_users: set = set() 
users_database: Dict[int, Dict] = {} 
bot_stats = {"start_time": time.time(), "total_searches": 0, "algolia_searches": 0}
RATE_LIMIT_SECONDS = 1 

def check_rate_limit(user_id: int) -> bool:
    current_time = time.time()
    if user_id in user_sessions and current_time - user_sessions[user_id].get('last_action', 0) < RATE_LIMIT_SECONDS:
        return False
    user_sessions[user_id]['last_action'] = current_time
    return True

def add_user(user_id: int, username: Optional[str] = None, first_name: Optional[str] = None):
    if user_id not in users_database:
        users_database[user_id] = {"user_id": user_id}

def algolia_fuzzy_search(query: str, limit: int = 20) -> List[Dict]:
    global algolia_index, bot_stats
    if not algolia_index: 
        print("⚠️ Algolia not initialized. Cannot perform search.")
        return []
    
    bot_stats["total_searches"] += 1
    
    try:
        search_results = algolia_index.search(
            query,
            {'attributesToRetrieve': ['title', 'post_id'], 'hitsPerPage': limit}
        )
        bot_stats["algolia_searches"] += 1
        
        results = []
        for hit in search_results['hits']:
            if hit.get('post_id'):
                results.append({"title": hit.get('title', 'Unknown Movie'), "post_id": hit['post_id']})
        return results
        
    except Exception as e:
        print(f"Error searching with Algolia: {e}")
        return []

async def add_movie_to_db_and_algolia(title: str, post_id: int):
    """Handles automatic indexing of new channel posts."""
    global algolia_index
    if not algolia_index or not SessionLocal: 
        print("⚠️ Indexing failed: DB/Algolia not initialized.")
        return False
        
    def sync_data():
        db_session = SessionLocal()
        try:
            existing_movie = db_session.query(Movie).filter(Movie.post_id == post_id).first()
            if existing_movie: 
                return False

            new_movie = Movie(title=title.strip(), post_id=post_id)
            db_session.add(new_movie)
            db_session.commit()
            db_session.refresh(new_movie)

            algolia_index.save_object({
                "objectID": new_movie.id,
                "title": title.strip(),
                "post_id": post_id,
            })
            
            print(f"✅ Auto-Indexed: {title} (Post ID: {post_id})")
            return True
            
        except Exception as e:
            db_session.rollback()
            print(f"❌ Error adding movie to DB/Algolia: {e}")
            return False
        finally:
            db_session.close()

    return await asyncio.to_thread(sync_data)

# ====================================================================
# TELEGRAM HANDLERS
# (kept unchanged from your version)
# ====================================================================

# ... KEEP ALL HANDLERS THE SAME ...

# ====================================================================
# FLASK SERVER (Health Check)
# ====================================================================

app_flask = Flask(__name__)

@app_flask.route('/', methods=['GET', 'POST'])
def health_check():
    global bot_stats
    uptime_seconds = int(time.time() - bot_stats["start_time"])
    db_status = "connected" if engine and SessionLocal else "disconnected"
    algolia_status = "connected" if algolia_index else "disconnected"
    
    return jsonify({
        "status": "ok", 
        "service": "telegram_bot_poller", 
        "searches_total": bot_stats['total_searches'], 
        "uptime_seconds": uptime_seconds,
        "database": db_status,
        "algolia": algolia_status
    })

@app_flask.route('/health', methods=['GET'])
def health_endpoint():
    """Health check endpoint for monitoring."""
    return jsonify({
        "status": "healthy",
        "timestamp": time.time(),
        "uptime": int(time.time() - bot_stats["start_time"])
    })

def start_flask_server():
    print(f"🌐 Starting Flask server on port {WEB_SERVER_PORT} for health checks...")
    app_flask.run(host='0.0.0.0', port=WEB_SERVER_PORT, debug=False, use_reloader=False)

# ====================================================================
# MAIN BOT RUNNER
# ====================================================================

async def start_polling_and_run():
    """Start the bot with improved webhook deletion and comprehensive logging."""
    if DEMO_MODE or not bot:
        print("⚠️  DEMO MODE: Telegram bot polling disabled")
        print("⚠️  Only Flask health check server is running")
        print("⚠️  Set BOT_TOKEN, DATABASE_URL, ALGOLIA_APPLICATION_ID, ALGOLIA_SEARCH_KEY in Render to activate bot")
        while True:
            await asyncio.sleep(1)
        return
    
    print("=" * 70)
    print("🤖 STARTING TELEGRAM BOT")
    print("=" * 70)
    
    # Step 1: Verify bot token is valid
    try:
        print("🔐 Verifying bot token...")
        bot_info = await bot.get_me()
        print(f"✅ Bot authenticated successfully!")
        print(f"   📛 Bot name: @{bot_info.username}")
        print(f"   🆔 Bot ID: {bot_info.id}")
    except Exception as token_error:
        print(f"❌ CRITICAL: Bot token verification failed!")
        print(f"❌ Error: {token_error}")
        print(f"❌ Traceback: {traceback.format_exc()}")
        print("=" * 70)
        print("🔄 Flask server will continue running for health checks only")
        while True:
            await asyncio.sleep(1)
        return
    
    # Step 2: Delete any existing webhook
    print("🔄 Checking for existing webhooks...")
    try:
        webhook_info = await bot.get_webhook_info()
        print(f"   📡 Current webhook URL: {webhook_info.url if getattr(webhook_info, 'url', None) else 'None'}")
        
        if getattr(webhook_info, 'url', None):
            print("   🗑️  Webhook detected! Deleting to enable polling...")
            delete_result = await bot.delete_webhook(drop_pending_updates=True)
            print(f"   ✅ Webhook deletion result: {delete_result}")
            await asyncio.sleep(3)
            print("   ✅ Webhook successfully deleted!")
        else:
            print("   ✅ No webhook configured. Ready for polling.")
    except Exception as webhook_error:
        print(f"   ⚠️  Warning during webhook check: {webhook_error}")
        print("   ⚠️  Attempting to delete webhook anyway...")
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            await asyncio.sleep(2)
            print("   ✅ Webhook deletion attempted successfully")
        except Exception as e:
            print(f"   ⚠️  Could not delete webhook: {e}")
    
    # Step 3: Register handlers (safe count)
    print("📝 Registering message handlers...")
    try:
        # avoid internal attributes which may differ across aiogram versions
        handler_count = len(getattr(dp, 'handlers', [])) if hasattr(dp, 'handlers') else 'unknown'
        print(f"   ✅ Handlers registered (approx): {handler_count}")
    except Exception:
        print("   ✅ Handlers registered (could not determine count safely)")
    
    # Step 4: Start polling
    print("🔄 Starting Long Polling mode...")
    print("=" * 70)
    
    try:
        print("✅ BOT IS NOW LISTENING FOR MESSAGES!")
        print("=" * 70)
        print("📨 Waiting for incoming messages...")
        print("=" * 70)
        
        # Start polling with allowed updates
        await dp.start_polling(
            bot, 
            allowed_updates=dp.resolve_used_update_types(),
            drop_pending_updates=True
        )
    except Exception as polling_error:
        print("=" * 70)
        print(f"❌ CRITICAL ERROR in polling: {polling_error}")
        print(f"❌ Error type: {type(polling_error).__name__}")
        print(f"❌ Traceback:\n{traceback.format_exc()}")
        print("=" * 70)
        print("🔄 Bot will attempt to keep Flask server alive for health checks...")
        while True:
            await asyncio.sleep(1)

if __name__ == "__main__":
    print("=" * 70)
    print("🚀 TELEGRAM MOVIE BOT - RENDER DEPLOYMENT")
    print("=" * 70)
    
    # Start Flask in a separate daemon thread
    flask_thread = threading.Thread(target=start_flask_server, daemon=True)
    flask_thread.start()
    print("✅ Flask health check server started in background")
    
    # 🚨 FIX APPLIED: Waiting 2 seconds to ensure Flask thread starts properly
    time.sleep(2) 
    
    # Start the bot polling
    try:
        asyncio.run(start_polling_and_run())
    except KeyboardInterrupt:
        print("\n⚠️ Bot stopped by user.")
    except Exception as main_error:
        print(f"❌ FATAL ERROR in main: {main_error}")
        print(f"❌ Traceback:\n{traceback.format_exc()}")
        print("🔄 Attempting to keep Flask server alive...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n⚠️ Process terminated.")
