import os
import json
import time
import asyncio
import threading
from typing import Dict, List
from collections import defaultdict
from flask import Flask, jsonify

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.enums import ParseMode

# --- Database and Search Imports ---
import firebase_admin
from firebase_admin import credentials, firestore
from algoliasearch.search_client import SearchClient
# --- Legacy search libraries (kept for dependency compatibility) ---
from rapidfuzz import fuzz 
# ------------------------------------

# --- CONFIGURATION (Load from Render Environment Variables) ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
WEB_SERVER_PORT = int(os.environ.get("PORT", 8080))
ADMIN_IDS = [7263519581] 

# Database/Search Keys 
FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID")
FIREBASE_PRIVATE_KEY_JSON_STR = os.getenv("FIREBASE_PRIVATE_KEY") # Single-line string with \n
ALGOLIA_APP_ID = os.getenv("ALGOLIA_APPLICATION_ID")
ALGOLIA_SEARCH_KEY = os.getenv("ALGOLIA_SEARCH_KEY") # Search-Only Key
ALGOLIA_INDEX_NAME = os.getenv("ALGOLIA_INDEX_NAME", "Media_index")

# Channel Details (MUST BE PRIVATE)
LIBRARY_CHANNEL_USERNAME = os.getenv("LIBRARY_CHANNEL_USERNAME", "MOVIEMAZA19")
LIBRARY_CHANNEL_ID = int(os.getenv("LIBRARY_CHANNEL_ID", -1002970735025))
JOIN_CHANNEL_USERNAME = os.getenv("JOIN_CHANNEL_USERNAME", "MOVIEMAZASU")
JOIN_GROUP_USERNAME = os.getenv("JOIN_GROUP_USERNAME", "THEGREATMOVIESL9")

if not BOT_TOKEN or not ALGOLIA_APP_ID or not ALGOLIA_SEARCH_KEY or not FIREBASE_PRIVATE_KEY_JSON_STR:
    raise ValueError("Missing essential environment variables (DB/Token)")

# --- INITIALIZATION ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
db = None
algolia_index = None

try:
    # Firebase Initialization
    cred_dict = json.loads(FIREBASE_PRIVATE_KEY_JSON_STR.replace('\\n', '\n'))
    cred = credentials.Certificate(cred_dict)
    firebase_admin.initialize_app(cred, {'projectId': FIREBASE_PROJECT_ID})
    db = firestore.client()
    
    # Algolia Initialization (using Search-Only Key for bot operations)
    algolia_client = SearchClient.create(ALGOLIA_APP_ID, ALGOLIA_SEARCH_KEY)
    algolia_index = algolia_client.init_index(ALGOLIA_INDEX_NAME)
    print("✅ Firebase & Algolia Clients Initialized.")
except Exception as e:
    print(f"❌ FATAL: Error initializing DB/Search. Check keys! Error: {e}")

# Global State Management
user_sessions: Dict[int, Dict] = defaultdict(dict)
verified_users: set = set() 
users_database: Dict[int, Dict] = {} 
bot_stats = {
    "start_time": time.time(),
    "total_searches": 0,
    "algolia_searches": 0,
    "db_movies_count": 0 # Will be updated dynamically
}
RATE_LIMIT_SECONDS = 1 

# --- CORE LOGIC FUNCTIONS (Unchanged from previous final version) ---

def check_rate_limit(user_id: int) -> bool:
    current_time = time.time()
    if user_id in user_sessions and current_time - user_sessions[user_id].get('last_action', 0) < RATE_LIMIT_SECONDS:
        return False
    user_sessions[user_id]['last_action'] = current_time
    return True

def add_user(user_id: int, username: str = None, first_name: str = None):
    user_id_str = str(user_id)
    if user_id_str not in users_database:
        users_database[user_id_str] = {"user_id": user_id}

def algolia_fuzzy_search(query: str, limit: int = 20) -> List[Dict]:
    global algolia_index, bot_stats
    if not algolia_index:
        return []

    bot_stats["total_searches"] += 1
    
    try:
        search_results = algolia_index.search(
            query,
            {
                'attributesToRetrieve': ['title', 'post_id'],
                'hitsPerPage': limit
            }
        )
        bot_stats["algolia_searches"] += 1
        
        results = []
        for hit in search_results['hits']:
            if hit.get('post_id'):
                results.append({
                    "title": hit.get('title', 'Unknown Title'),
                    "post_id": hit['post_id']
                })
        return results
        
    except Exception as e:
        print(f"Error searching with Algolia: {e}")
        return []

async def add_movie_to_db_and_algolia(title: str, post_id: int):
    """Handles automatic indexing of new channel posts."""
    if not db or not algolia_index:
        print("Database/Search not ready for indexing.")
        return False
        
    def sync_data():
        try:
            # 1. Check for duplicate post_id (Critical for re-uploads)
            existing_doc = db.collection('movies').where('post_id', '==', post_id).limit(1).get()
            if existing_doc:
                print(f"⚠️ Duplicate post_id skipped: {title} (Post ID: {post_id})")
                return False

            # 2. Add to Firestore (Master data)
            doc_ref = db.collection('movies').add({
                "title": title.strip(),
                "post_id": post_id,
                "created_at": firestore.SERVER_TIMESTAMP
            })
            
            doc_id = doc_ref[1].id

            # 3. Add to Algolia Index (Search data)
            algolia_index.save_object({
                "objectID": doc_id, 
                "title": title.strip(),
                "post_id": post_id,
            })
            
            print(f"✅ Auto-Indexed: {title} (Post ID: {post_id})")
            return True
            
        except Exception as e:
            print(f"❌ Error adding movie to DB/Algolia: {e}")
            return False

    await asyncio.to_thread(sync_data)

# --- TELEGRAM HANDLERS (Unchanged User and Core Logic) ---

@dp.message(Command("start"))
async def cmd_start(message: Message):
    if message.from_user:
        add_user(
            user_id=message.from_user.id,
            username=message.from_user.username,
            first_name=message.from_user.first_name
        )
    
    if message.from_user and message.from_user.id not in verified_users and message.from_user.id not in ADMIN_IDS:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🔗 Join Channel", url=f"https://t.me/{JOIN_CHANNEL_USERNAME}")],
            [InlineKeyboardButton(text=f"👥 Join Group", url=f"https://t.me/{JOIN_GROUP_USERNAME}")],
            [InlineKeyboardButton(text="✅ I Joined", callback_data="joined")]
        ])
        
        await message.answer(
            "नमस्ते! सर्च करने के लिए 'I Joined' पर क्लिक करें।",
            reply_markup=keyboard
        )
    
    else:
        await message.answer(
            "नमस्ते! 20 सबसे सटीक परिणामों के लिए फिल्म का नाम टाइप करें। \n"
            "🛡️ **Safe Access:** क्लिक करने पर आपको प्रतिबंधित (Restricted) डाउनलोड लिंक मिलेगा।"
        )

@dp.callback_query(F.data == "joined")
async def process_joined(callback: types.CallbackQuery):
    if callback.from_user:
        verified_users.add(callback.from_user.id)
        
    welcome_text = "✅ एक्सेस मिल गया! अब आप फिल्में खोज सकते हैं।"
    
    if callback.message and isinstance(callback.message, Message):
        await callback.message.edit_text(welcome_text, reply_markup=None) 
    await callback.answer("✅ Access granted! You can now start searching.")

@dp.message(F.text)
async def handle_search(message: Message):
    try:
        if not message.text or message.text.startswith('/'): return
        
        query = message.text.strip()
        user_id = message.from_user.id
        
        if user_id not in ADMIN_IDS and user_id not in verified_users:
            await cmd_start(message)
            return
            
        if not check_rate_limit(user_id): return
        
        results = algolia_fuzzy_search(query, limit=20)
        
        if not results:
            await message.answer(f"❌ कोई मूवी नहीं मिली: **{query}**", parse_mode=ParseMode.MARKDOWN)
            return
        
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
    
    except Exception as e:
        print(f"Error in handle_search: {e}")
        await message.answer("❌ सर्च में कोई त्रुटि हुई।")

@dp.callback_query(F.data.startswith("post_"))
async def send_movie_link(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        
        if user_id not in ADMIN_IDS and user_id not in verified_users:
             await callback.answer("🛑 पहुँच वर्जित (Access Denied)।")
             return

        try:
            post_id = int(callback.data.split('_')[1])
        except (ValueError, IndexError):
            await callback.answer("❌ गलत चुनाव।")
            return
        
        # RENDER/POLICY COMPLIANT LINK GENERATION
        channel_id_clean = str(LIBRARY_CHANNEL_ID).replace("-100", "") 
        post_url = f"https://t.me/c/{channel_id_clean}/{post_id}"
        
        # Cleanup last search message
        if 'last_search_msg' in user_sessions.get(user_id, {}):
            try:
                await bot.delete_message(chat_id=user_id, message_id=user_sessions[user_id]['last_search_msg'])
            except: pass
        
        # Send the final download link button
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬇️ Movie Download Link", url=post_url)]
        ])
        
        await bot.send_message(
            chat_id=user_id,
            text="✅ **डाउनलोड लिंक तैयार है!**\n\n"
                 "यह लिंक आपको सीधे मूवी पोस्ट पर ले जाएगा।",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
        
        await callback.answer("✅ लिंक भेज दिया गया है।")
        
    except Exception as e:
        print(f"Error sending movie link: {e}")
        await callback.answer("❌ लिंक बनाने में त्रुटि हुई।")


@dp.channel_post()
async def handle_channel_post(message: Message):
    """Automatically indexes new media posts from the library channel."""
    try:
        if not message.chat or message.chat.id != LIBRARY_CHANNEL_ID: return
            
        if message.document or message.video:
            caption = message.caption or ""
            title = caption.split('\n')[0].strip() if caption else "Unknown Movie"
            post_id = message.message_id 
            
            if title and title != "Unknown Movie" and post_id:
                await add_movie_to_db_and_algolia(title, post_id)
                
    except Exception as e:
        print(f"Error in handle_channel_post: {e}")

# --- ADMIN COMMANDS (New Commands Added) ---

@dp.message(Command("refresh"))
async def cmd_refresh(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ You are not authorized to use this command.")
        return
    await message.answer("✅ Cloud services are active. Auto-indexing is on.") # Modified response since loading from local is removed

# New Command 1
@dp.message(Command("total_movies"))
async def cmd_total_movies(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ You are not authorized to use this command.")
        return
    
    if not db:
        await message.answer("❌ Database connection failed.")
        return
    
    try:
        # Get live count from Firestore
        count_result = await asyncio.to_thread(lambda: db.collection('movies').count().get())
        movie_count = count_result[0][0].value
        
        await message.answer(f"📊 Live Indexed Movies in DB: **{movie_count}**", parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await message.answer(f"❌ Error fetching movie count: {e}")

# New Command 2
@dp.message(Command("help"))
async def cmd_help(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS:
        await message.answer("नमस्ते! फिल्म का नाम टाइप करें और 20 सबसे सटीक परिणाम पाएँ।")
        return
        
    help_text = (
        "🎬 **Admin Panel Commands:**\n\n"
        "1. **/stats** - Bot के प्रदर्शन (performance) के आँकड़े देखें।\n"
        "2. **/broadcast [Message/Photo/Video]** - सभी यूज़र्स को संदेश भेजें।\n"
        "3. **/total_movies** - Firebase में Indexed Movies की लाइव संख्या देखें।\n"
        "4. **/refresh** - Cloud service status चेक करें (मुख्य रूप से अब इसकी आवश्यकता नहीं)।\n\n"
        "ℹ️ **User Logic:**\n"
        "   * New Uploads (Channel Post) **Automatically Index** होते हैं।\n"
        "   * Search **Algolia** द्वारा 20 सबसे सटीक परिणामों के साथ चलता है।\n"
        "   * Link Generation **Render-Safe** है (Restricted Post URL)।"
    )
    await message.answer(help_text, parse_mode=ParseMode.MARKDOWN)


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ You are not authorized to use this command.")
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
        await message.answer("⛔ You are not authorized to use this command.")
        return
    
    broadcast_text = message.text.replace("/broadcast", "").strip()
    broadcast_photo = None
    broadcast_video = None
    
    # Logic to capture photo/video if replying to a message
    if message.reply_to_message:
        if message.reply_to_message.photo:
            broadcast_photo = message.reply_to_message.photo[-1].file_id
            if message.reply_to_message.caption:
                broadcast_text = broadcast_text or message.reply_to_message.caption
        elif message.reply_to_message.video:
            broadcast_video = message.reply_to_message.video.file_id
            if message.reply_to_message.caption:
                broadcast_text = broadcast_text or message.reply_to_message.caption
    
    if not broadcast_text and not broadcast_photo and not broadcast_video:
        help_text = "⚠️ Broadcast Usage: Reply to a photo/video with /broadcast or type /broadcast [Your message here]."
        await message.answer(help_text)
        return
    
    if not users_database:
        await message.answer("⚠️ No users in database yet.")
        return
    
    sent_count = 0
    blocked_count = 0
    
    media_type = "📸 photo" if broadcast_photo else ("🎥 video" if broadcast_video else "📝 text")
    status_msg = await message.answer(f"📡 Broadcasting {media_type} to {len(users_database)} users...")
    
    for user_id_str, user_data in users_database.items():
        try:
            user_id = int(user_id_str)
            
            # Use appropriate send function
            if broadcast_photo:
                await bot.send_photo(chat_id=user_id, photo=broadcast_photo, caption=f"📢 Broadcast:\n\n{broadcast_text}")
            elif broadcast_video:
                await bot.send_video(chat_id=user_id, video=broadcast_video, caption=f"📢 Broadcast:\n\n{broadcast_text}")
            else:
                await bot.send_message(chat_id=user_id, text=f"📢 Broadcast:\n\n{broadcast_text}")
            
            sent_count += 1
            await asyncio.sleep(0.05) # Small delay to avoid rate limiting
        except Exception as e:
            error_msg = str(e).lower()
            if "blocked" in error_msg or "deactivated" in error_msg:
                blocked_count += 1
            print(f"Failed to send to {user_id}: {e}")
    
    summary = (
        "✅ **Broadcast Complete!**\n\n"
        f"✅ Sent: {sent_count}\n"
        f"🚫 Blocked/Failed: {blocked_count + (len(users_database) - sent_count - blocked_count)}\n"
        f"👥 Total Users: {len(users_database)}"
    )
    
    await status_msg.edit_text(summary, parse_mode=ParseMode.MARKDOWN)


# --- FLASK SERVER & DEPLOYMENT SETUP ---

app_flask = Flask(__name__)

@app_flask.route('/', methods=['GET', 'POST'])
def health_check():
    global bot_stats
    uptime_seconds = int(time.time() - bot_stats["start_time"])
    
    return jsonify({
        "status": "ok",
        "service": "telegram_bot_poller",
        "searches_total": bot_stats['total_searches'],
        "uptime_seconds": uptime_seconds
    })

def start_flask_server():
    print(f"Starting Flask server on port {WEB_SERVER_PORT} for health checks...")
    app_flask.run(host='0.0.0.0', port=WEB_SERVER_PORT, debug=False, use_reloader=False)

async def start_polling_and_run():
    print("Deleting old Telegram Webhook...")
    await bot.delete_webhook(drop_pending_updates=True) 
    print("Webhook deleted successfully. Starting Long Polling...")
    
    try:
        await dp.start_polling(bot)
    except Exception as e:
        print(f"FATAL ERROR: Bot could not start polling. {e}")

if __name__ == "__main__":
    # 1. Start Flask Health Check in a separate thread
    flask_thread = threading.Thread(target=start_flask_server)
    flask_thread.daemon = True 
    flask_thread.start()
    
    # 2. Start Telegram Polling in the main thread
    try:
        asyncio.run(start_polling_and_run())
    except Exception as e:
        print(f"FATAL ERROR: Bot process ended. {e}")
