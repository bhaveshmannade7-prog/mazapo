import os
import json
import time
import asyncio
import threading
import sys 
import traceback
import logging
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

# 🚨 CORRECT LIBRARY CHANNEL ID (Fixed default value)
CORRECT_LIBRARY_CHANNEL_ID = -1003138949015 

LIBRARY_CHANNEL_USERNAME = os.getenv("LIBRARY_CHANNEL_USERNAME", "MOVIEMAZA19")
# LIBRARY_CHANNEL_ID will use the correct ID as default if not set in Env.
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", CORRECT_LIBRARY_CHANNEL_ID))
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "MOVIEMAZASU")
JOIN_GROUP_USERNAME = os.getenv("JOIN_GROUP_USERNAME", "THEGREATMOVIESL9")

if not BOT_TOKEN or not ALGOLIA_APP_ID or not ALGOLIA_SEARCH_KEY or not DATABASE_URL:
    print("⚠️  WARNING: Missing essential environment variables (DB/Token)")
    print("⚠️  Running in DEMO MODE - bot functionality will be limited")
    print("⚠️  For production, set: BOT_TOKEN, DATABASE_URL, ALGOLIA_APPLICATION_ID, ALGOLIA_SEARCH_KEY")
    
    if not BOT_TOKEN:
        BOT_TOKEN = "demo_token_placeholder"
    if not DATABASE_URL:
        DATABASE_URL = "postgresql://demo:demo@localhost/demo"
    if not ALGOLIA_APP_ID:
        ALGOLIA_APP_ID = "demo_app_id"
    if not ALGOLIA_SEARCH_KEY:
        ALGOLIA_SEARCH_KEY = "demo_search_key"

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
    print(f"⚠️  Could not initialize bot (likely demo mode): {e}")
    print("⚠️  Bot will run as health-check server only")
    DEMO_MODE = True

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
            if db_url.startswith("postgresql://"):
                db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
            
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
            
            algolia_client = SearchClient.create(ALGOLIA_APP_ID, ALGOLIA_SEARCH_KEY)
            algolia_index = algolia_client.init_index(ALGOLIA_INDEX_NAME)
            
            try:
                algolia_index.search("test", {'hitsPerPage': 1})
                print("✅ Algolia connection verified.")
            except Exception as e:
                print(f"⚠️ Algolia health check warning: {e}")
            
            print("✅ PostgreSQL & Algolia Clients Initialized Successfully.")
            return True

        except Exception as e:
            print(f"❌ Initialization attempt {attempt + 1} failed: {e}")
            
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
# ====================================================================

@dp.message(Command("start"))
async def cmd_start(message: Message):
    print(f"📨 Received /start from user {message.from_user.id if message.from_user else 'Unknown'}")
    
    if message.from_user:
        user_id = message.from_user.id
        add_user(user_id=user_id, username=message.from_user.username, first_name=message.from_user.first_name)
    else: 
        return
    
    if user_id in ADMIN_IDS:
        uptime_seconds = int(time.time() - bot_stats["start_time"])
        hours = uptime_seconds // 3600
        minutes = (uptime_seconds % 3600) // 60
        
        admin_welcome_text = (
            f"👑 **Welcome, Admin! Bot is LIVE.**\n"
            f"────────────────────────\n"
            f"🟢 **Status:** Operational\n"
            f"⏱ **Uptime:** {hours}h {minutes}m\n"
            f"👥 **Active Users:** {len(users_database)}\n"
            f"🔍 **Total Searches:** {bot_stats['total_searches']}\n"
            f"────────────────────────\n"
            f"**Quick Commands:**\n"
            f"• /total_movies: DB में Indexed Movies की संख्या।\n"
            f"• /stats: विस्तृत प्रदर्शन (Performance) आँकड़े।\n"
            f"• /broadcast [संदेश]: सभी यूज़र्स को भेजें।\n"
            f"• /help: सभी कमांड्स की सूची।\n"
            f"• /cleanup_users: Inactive users को हटाएँ।\n"
            f"• /reload_config: Environment variables रीलोड करें।"
        )
        await message.answer(admin_welcome_text, parse_mode=ParseMode.MARKDOWN)
        print(f"✅ Sent admin welcome to user {user_id}")
        return 

    if user_id not in verified_users:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🔗 Join Channel", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}")],
            [InlineKeyboardButton(text=f"👥 Join Group", url=f"https://t.me/{JOIN_GROUP_USERNAME}")],
            [InlineKeyboardButton(text="✅ I Joined", callback_data="joined")]
        ])
        await message.answer("नमस्ते! सर्च करने के लिए 'I Joined' पर क्लिक करें।", reply_markup=keyboard)
        print(f"✅ Sent join prompt to user {user_id}")
    else:
        await message.answer("नमस्ते! 20 सबसे सटीक परिणामों के लिए फिल्म का नाम टाइप करें। \n🛡️ **Safe Access:** क्लिक करने पर आपको प्रतिबंधित (Restricted) डाउनलोड लिंक मिलेगा।")
        print(f"✅ Sent welcome message to verified user {user_id}")

@dp.callback_query(F.data == "joined")
async def process_joined(callback: types.CallbackQuery):
    print(f"📨 Received 'joined' callback from user {callback.from_user.id if callback.from_user else 'Unknown'}")
    
    if callback.from_user: 
        verified_users.add(callback.from_user.id)
        print(f"✅ User {callback.from_user.id} verified")
    welcome_text = "✅ एक्सेस मिल गया! अब आप फिल्में खोज सकते हैं।"
    if callback.message and isinstance(callback.message, Message):
        await callback.message.edit_text(welcome_text, reply_markup=None) 
    await callback.answer("✅ Access granted! You can now start searching.")

@dp.message(F.text)
async def handle_search(message: Message):
    try:
        if not message.text or message.text.startswith('/'): 
            return
        
        print(f"📨 Received search query from user {message.from_user.id}: '{message.text}'")
        
        query = message.text.strip()
        user_id = message.from_user.id
        
        if user_id not in ADMIN_IDS and user_id not in verified_users: 
            print(f"⚠️ User {user_id} not verified, showing join prompt")
            await cmd_start(message)
            return
        if not check_rate_limit(user_id): 
            print(f"⚠️ Rate limit hit for user {user_id}")
            return
        
        print(f"🔍 Searching Algolia for: '{query}'")
        results = algolia_fuzzy_search(query, limit=20)
        
        if not results:
            await message.answer(f"❌ कोई मूवी नहीं मिली: **{query}**", parse_mode=ParseMode.MARKDOWN)
            print(f"❌ No results found for: '{query}'")
            return
        
        print(f"✅ Found {len(results)} results for: '{query}'")
        
        keyboard_buttons = []
        for result in results:
            button_text = f"🎬 {result['title']}"
            callback_data = f"post_{result['post_id']}"
            keyboard_buttons.append([InlineKeyboardButton(text=button_text, callback_data=callback_data)])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
        sent_msg = await message.answer(
            f"🔍 **{len(keyboard_buttons)}** परिणाम मिले: **{query}**",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
        user_sessions[user_id]['last_search_msg'] = sent_msg.message_id
        print(f"✅ Sent {len(keyboard_buttons)} results to user {user_id}")
    
    except Exception as e:
        print(f"❌ ERROR in handle_search: {e}")
        print(f"❌ Traceback: {traceback.format_exc()}")
        await message.answer("❌ सर्च में कोई त्रुटि हुई।")

@dp.callback_query(F.data.startswith("post_"))
async def send_movie_link(callback: types.CallbackQuery):
    try:
        print(f"📨 Received post callback from user {callback.from_user.id}: {callback.data}")
        
        user_id = callback.from_user.id
        if user_id not in ADMIN_IDS and user_id not in verified_users: 
            await callback.answer("🛑 पहुँच वर्जित (Access Denied)。")
            print(f"⚠️ Unverified user {user_id} tried to access movie")
            return

        try: 
            post_id = int(callback.data.split('_')[1])
        except (ValueError, IndexError): 
            await callback.answer("❌ गलत चुनाव।")
            return
        
        channel_id_clean = str(LIBRARY_CHANNEL_ID).replace("-100", "") 
        post_url = f"https://t.me/c/{channel_id_clean}/{post_id}"
        
        if 'last_search_msg' in user_sessions.get(user_id, {}):
            try: 
                await bot.delete_message(chat_id=user_id, message_id=user_sessions[user_id]['last_search_msg'])
            except: 
                pass
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬇️ Movie Download Link", url=post_url)]
        ])
        
        await bot.send_message(
            chat_id=user_id,
            text="✅ **डाउनलोड लिंक तैयार है!**\n\nयह लिंक आपको सीधे मूवी पोस्ट पर ले जाएगा。",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
        await callback.answer("✅ लिंक भेज दिया गया है।")
        print(f"✅ Sent movie link (post {post_id}) to user {user_id}")
        
    except Exception as e:
        print(f"❌ ERROR in send_movie_link: {e}")
        print(f"❌ Traceback: {traceback.format_exc()}")
        await callback.answer("❌ लिंक बनाने में त्रुटि हुई है।")

@dp.channel_post()
async def handle_channel_post(message: Message):
    try:
        if not message.chat or message.chat.id != LIBRARY_CHANNEL_ID: 
            return
        if message.document or message.video:
            caption = message.caption or ""
            title = caption.split('\n')[0].strip() if caption else "Unknown Movie"
            post_id = message.message_id 
            
            if title and title != "Unknown Movie" and post_id:
                await add_movie_to_db_and_algolia(title, post_id)
    except Exception as e:
        print(f"Error in handle_channel_post: {e}")

# ====================================================================
# ADMIN HANDLERS 
# ====================================================================

@dp.message(Command("refresh"))
async def cmd_refresh(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: 
        return
    await message.answer("✅ Cloud services are active. Auto-indexing is on.") 

@dp.message(Command("cleanup_users"))
async def cmd_cleanup_users(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: 
        return
    
    old_count = len(users_database)
    users_database.clear()
    await message.answer(f"🧹 Cleaned up in-memory user list. Cleared **{old_count}** entries.", parse_mode=ParseMode.MARKDOWN)

@dp.message(Command("reload_config"))
async def cmd_reload_config(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: 
        return
    
    await message.answer("🔄 Config status: Environment variables are static in Render. To apply changes, please manually redeploy the service.")

@dp.message(Command("total_movies"))
async def cmd_total_movies(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: 
        return
    if not engine: 
        await message.answer("❌ Database connection failed.")
        return
    try:
        db_gen = get_db()
        db_session = next(db_gen, None)
        if db_session:
            count = db_session.query(Movie).count()
            db_session.close()
            await message.answer(f"📊 Live Indexed Movies in DB: **{count}**", parse_mode=ParseMode.MARKDOWN)
        else:
            await message.answer("❌ Database session unavailable.")
    except Exception as e:
        await message.answer(f"❌ Error fetching movie count: {e}")

@dp.message(Command("help"))
async def cmd_help(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: 
        await message.answer("नमस्ते! फिल्म का नाम टाइप करें और 20 सबसे सटीक परिणाम पाएँगे।")
        return
        
    help_text = (
        "🎬 **Admin Panel Commands:**\n\n"
        "1. **/stats** - Bot के प्रदर्शन (performance) के आँकडे देखें।\n"
        "2. **/broadcast [Message/Photo/Video]** - सभी यूज़र्स को संदेश भेजें।\n"
        "3. **/total_movies** - Database में Indexed Movies की लाइव संख्या देखें।\n"
        "4. **/refresh** - Cloud service status चेक करें।\n"
        "5. **/cleanup_users** - Inactive users को हटाएँ।\n"
        "6. **/reload_config** - Environment variables की स्थिति देखें।\n\n"
        "ℹ️ **User Logic:** Search **Algolia** द्वारा 20 परिणामों के साथ चलता है। Link Generation **Render-Safe** है।"
    )
    await message.answer(help_text, parse_mode=ParseMode.MARKDOWN)

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: 
        return
    uptime_seconds = int(time.time() - bot_stats["start_time"])
    hours = uptime_seconds // 3600
    minutes = (uptime_seconds % 3600) // 60
    
    stats_text = (
        "📊 **Bot Statistics (Live):**\n\n"
        f"🔍 Total Searches: {bot_stats['total_searches']}\n"
        f"⚡ Algolia Searches: {bot_stats['algolia_searches']}\n"
        f"👥 Total Unique Users: {len(users_database)}\n"
        f"⏱ Uptime: {hours}h {minutes}m"
    )
    await message.answer(stats_text, parse_mode=ParseMode.MARKDOWN)

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS: 
        return
    broadcast_text = message.text.replace("/broadcast", "").strip()
    broadcast_photo, broadcast_video = None, None
    if message.reply_to_message:
        if message.reply_to_message.photo: 
            broadcast_photo = message.reply_to_message.photo[-1].file_id
        elif message.reply_to_message.video: 
            broadcast_video = message.reply_to_message.video.file_id
        if message.reply_to_message.caption: 
            broadcast_text = broadcast_text or message.reply_to_message.caption
    
    if not broadcast_text and not broadcast_photo and not broadcast_video:
        await message.answer("⚠️ Broadcast Usage: Reply to a photo/video with /broadcast or type /broadcast [Your message here].")
        return
    
    if not users_database: 
        await message.answer("⚠️ No users in database yet.")
        return
    
    sent_count, blocked_count = 0, 0
    media_type = "📸 photo" if broadcast_photo else ("🎥 video" if broadcast_video else "📝 text")
    status_msg = await message.answer(f"📡 Broadcasting {media_type} to {len(users_database)} users...")
    
    for user_id_key, user_data in users_database.items():
        try:
            target_user_id = int(user_id_key)
            if broadcast_photo: 
                await bot.send_photo(chat_id=target_user_id, photo=broadcast_photo, caption=f"📢 Broadcast:\n\n{broadcast_text}")
            elif broadcast_video: 
                await bot.send_video(chat_id=target_user_id, video=broadcast_video, caption=f"📢 Broadcast:\n\n{broadcast_text}")
            else: 
                await bot.send_message(chat_id=target_user_id, text=f"📢 Broadcast:\n\n{broadcast_text}")
            sent_count += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            if "blocked" in str(e).lower() or "deactivated" in str(e).lower(): 
                blocked_count += 1
            try:
                print(f"Failed to send to {target_user_id}: {e}")
            except:
                print(f"Failed to send broadcast: {e}")
    
    summary = (
        "✅ **Broadcast Complete!**\n\n" 
        f"✅ Sent: {sent_count}\n" 
        f"🚫 Blocked/Failed: {blocked_count + (len(users_database) - sent_count - blocked_count)}\n" 
        f"👥 Total Users: {len(users_database)}"
    )
    await status_msg.edit_text(summary, parse_mode=ParseMode.MARKDOWN)

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
    # 🚨 FIX 1: Flask logging ko kam karo taaki Render logs clear rahein
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR) # Sirf errors log ho, INFO nahi
    
    # Bind to 0.0.0.0 to ensure Render sees the port
    app_flask.run(host='0.0.0.0', port=WEB_SERVER_PORT, debug=False, use_reloader=False)

# ====================================================================
# MAIN BOT RUNNER
# ====================================================================

async def start_polling_and_run():
    """Start the bot with improved webhook deletion and comprehensive logging."""
    # Print start sequence for debugging
    print("=" * 70)
    print("🤖 STARTING TELEGRAM BOT")
    print("=" * 70)

    if DEMO_MODE or not bot:
        print("⚠️  DEMO MODE: Telegram bot polling disabled")
        print("⚠️  Only Flask health check server is running")
        print("⚠️  Set BOT_TOKEN, DATABASE_URL, ALGOLIA_APPLICATION_ID, ALGOLIA_SEARCH_KEY in Render to activate bot")
        while True:
            await asyncio.sleep(1)
        return
    
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
    
    # Step 2: Delete any existing webhook (This is confirmed deleted, but safety check)
    print("🔄 Checking for existing webhooks...")
    try:
        webhook_info = await bot.get_webhook_info()
        print(f"   📡 Current webhook URL: {webhook_info.url if webhook_info.url else 'None'}")
        
        if webhook_info.url:
            print("   🗑️  Webhook detected! Deleting to enable polling...")
            delete_result = await bot.delete_webhook(drop_pending_updates=True)
            print(f"   ✅ Webhook deletion result: {delete_result}")
            # Wait to ensure webhook is fully removed
            await asyncio.sleep(3)
            print("   ✅ Webhook successfully deleted!")
        else:
            print("   ✅ No webhook configured. Ready for polling.")
    except Exception as webhook_error:
        print(f"   ⚠️  Warning during webhook check: {webhook_error}")
        # Added traceback for better debugging on network issues
        print(f"   ❌ Traceback: {traceback.format_exc()}")
    
    # Step 3: Register handlers (verify they're registered)
    print("📝 Registering message handlers...")
    registered_handlers = len(dp.observers['message'])
    print(f"   ✅ {registered_handlers} message handlers registered")
    
    # Step 4: Start polling
    print("🔄 Starting Long Polling mode...")
    print("=" * 70)
    
    try:
        print("✅ BOT IS NOW LISTENING FOR MESSAGES! (Timeout: 60s)")
        print("=" * 70)
        print("📨 Waiting for incoming messages...")
        print("=" * 70)
        
        # Start polling with allowed updates
        # 🚨 FINAL FIX: Added request_timeout=60.0 for stability on Render/slow networks
        await dp.start_polling(
            bot, 
            allowed_updates=dp.resolve_used_update_types(),
            drop_pending_updates=True,
            request_timeout=60.0 
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

# ====================================================================
# MAIN EXECUTION (FIX: Re-added for Process Stability)
# ====================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("🚀 TELEGRAM MOVIE BOT - RENDER DEPLOYMENT")
    print("=" * 70)
    
    # 🚨 FIX: Start Flask in a non-daemon thread to ensure main process doesn't exit prematurely.
    flask_thread = threading.Thread(target=start_flask_server) 
    flask_thread.start()
    print("✅ Flask health check server started in background")
    
    # Increased delay for stability.
    time.sleep(3) 
    
    # The main process runs the Asyncio Bot Polling
    try:
        asyncio.run(start_polling_and_run())
    except KeyboardInterrupt:
        print("\n⚠️ Bot stopped by user.")
    except Exception as main_error:
        print(f"❌ FATAL ERROR in main: {main_error}")
        print(f"❌ Traceback:\n{traceback.format_exc()}")
        print("🔄 Attempting to keep Flask server alive...")
        # If bot fails, main process still enters infinite loop for Flask's sake.
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n⚠️ Process terminated.")
