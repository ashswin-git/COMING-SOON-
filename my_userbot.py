"""
╔══════════════════════════════════════════════════════╗
║     USERBOT - Customer Service (Your Own Account)    ║
║  - Apne channel se products fetch karo               ║
║  - Apne joined groups se customer queries fetch      ║
║  - Apne ID se customer ko directly reply karo        ║
╚══════════════════════════════════════════════════════╝

⚠️  NOTE: Yeh USERBOT hai — tumhara apna Telegram account use hoga.
    Bot token ki zaroorat nahi. Phone number se login hoga.
"""

import asyncio
import sqlite3
import logging
from datetime import datetime
from telethon import TelegramClient, events
from telethon.tl.types import Channel, Chat, User

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
API_ID   = 123456           # my.telegram.org
API_HASH = "your_api_hash"  # my.telegram.org

# Tumhara product channel username ya ID
# e.g. "my_product_channel" ya -1001234567890
PRODUCT_CHANNEL = "your_channel_username"

# Kitne messages channel se fetch karne hain
CHANNEL_FETCH_LIMIT = 100

# ─────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger(__name__)

# Userbot — apne account se login
client = TelegramClient("my_userbot_session", API_ID, API_HASH)

DB = "userbot.db"

# ═══════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════

def init_db():
    with sqlite3.connect(DB) as conn:
        c = conn.cursor()

        # Products from channel
        c.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_id      INTEGER UNIQUE,
                name        TEXT,
                full_text   TEXT,
                media_path  TEXT,
                fetched_at  TEXT
            )
        """)

        # Groups I am in
        c.execute("""
            CREATE TABLE IF NOT EXISTS my_groups (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id    INTEGER UNIQUE,
                group_name  TEXT,
                username    TEXT,
                member_count INTEGER,
                fetched_at  TEXT
            )
        """)

        # Customer queries from groups
        c.execute("""
            CREATE TABLE IF NOT EXISTS customer_queries (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                username    TEXT,
                full_name   TEXT,
                group_id    INTEGER,
                group_name  TEXT,
                query_text  TEXT,
                status      TEXT DEFAULT 'pending',
                replied_at  TEXT,
                timestamp   TEXT
            )
        """)

        conn.commit()
    log.info("✅ Database ready!")

def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

QUERY_KEYWORDS = [
    "price", "cost", "kitna", "rate", "buy", "kharidna",
    "available", "stock", "product", "detail", "info",
    "kya hai", "kaise", "batao", "bata", "chahiye",
    "how much", "what is", "tell me", "show me", "order",
    "delivery", "shipping", "discount", "offer"
]

def is_customer_query(text):
    t = text.lower()
    return any(kw in t for kw in QUERY_KEYWORDS)

# ═══════════════════════════════════════════
#  FETCH PRODUCTS FROM CHANNEL
# ═══════════════════════════════════════════

async def fetch_channel_products():
    """Apne channel ke saare posts fetch karo aur DB mein save karo"""
    log.info(f"📦 Channel se products fetch ho rahe hain: {PRODUCT_CHANNEL}")
    count = 0

    try:
        async for msg in client.iter_messages(PRODUCT_CHANNEL, limit=CHANNEL_FETCH_LIMIT):
            if not msg.text and not msg.media:
                continue

            text = msg.text or ""
            lines = text.strip().split("\n")
            name = lines[0][:200] if lines else f"Product #{msg.id}"

            with sqlite3.connect(DB) as conn:
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO products (msg_id, name, full_text, fetched_at)
                        VALUES (?, ?, ?, ?)
                    """, (msg.id, name, text, now()))
                    count += 1
                except:
                    pass

        log.info(f"✅ {count} products fetched from channel!")
        return count

    except Exception as e:
        log.error(f"❌ Channel fetch error: {e}")
        return 0

# ═══════════════════════════════════════════
#  FETCH ALL MY JOINED GROUPS
# ═══════════════════════════════════════════

async def fetch_my_groups():
    """Jitne bhi groups/channels mein ho, unki list fetch karo"""
    log.info("🏠 Joined groups fetch ho rahe hain...")
    groups = []

    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        if isinstance(entity, (Channel, Chat)):
            gid = dialog.id
            gname = dialog.name or "Unknown"
            username = getattr(entity, "username", None) or ""
            members = getattr(entity, "participants_count", 0) or 0

            with sqlite3.connect(DB) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO my_groups (group_id, group_name, username, member_count, fetched_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (gid, gname, username, members, now()))

            groups.append({"id": gid, "name": gname, "members": members})

    log.info(f"✅ {len(groups)} groups fetched!")
    return groups

# ═══════════════════════════════════════════
#  FETCH CUSTOMER QUERIES FROM ALL GROUPS
# ═══════════════════════════════════════════

async def fetch_group_queries(limit_per_group=50):
    """Har group ke recent messages scan karo aur queries nikalo"""
    log.info("🔍 Group messages mein customer queries dhundh raha hoon...")
    total = 0

    with sqlite3.connect(DB) as conn:
        c = conn.cursor()
        c.execute("SELECT group_id, group_name FROM my_groups")
        groups = c.fetchall()

    for gid, gname in groups:
        try:
            async for msg in client.iter_messages(gid, limit=limit_per_group):
                if not msg.text or not msg.sender_id:
                    continue
                if not is_customer_query(msg.text):
                    continue

                sender = await msg.get_sender()
                if not sender or getattr(sender, "bot", False):
                    continue

                uname = f"@{sender.username}" if getattr(sender, "username", None) else ""
                fname = f"{getattr(sender, 'first_name', '')} {getattr(sender, 'last_name', '')}".strip()

                with sqlite3.connect(DB) as conn:
                    # Avoid duplicate entries
                    c = conn.cursor()
                    c.execute("SELECT id FROM customer_queries WHERE user_id=? AND query_text=? AND group_id=?",
                              (sender.id, msg.text[:500], gid))
                    if not c.fetchone():
                        conn.execute("""
                            INSERT INTO customer_queries (user_id, username, full_name, group_id, group_name, query_text, timestamp)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (sender.id, uname, fname, gid, gname, msg.text[:500], now()))
                        total += 1

        except Exception as e:
            log.warning(f"Group {gname} ({gid}) error: {e}")
            continue

    log.info(f"✅ {total} new customer queries found!")
    return total

# ═══════════════════════════════════════════
#  SEND MESSAGE FROM YOUR OWN ID
# ═══════════════════════════════════════════

async def send_to_customer(user_id, message):
    """Apne account se directly customer ko message bhejo"""
    try:
        await client.send_message(user_id, message, parse_mode="markdown")
        log.info(f"✅ Message sent to {user_id}")
        return True
    except Exception as e:
        log.error(f"❌ Send error to {user_id}: {e}")
        return False


async def reply_to_pending_queries_interactively():
    """Pending queries dikhao aur reply karo"""
    with sqlite3.connect(DB) as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, full_name, username, user_id, group_name, query_text
            FROM customer_queries WHERE status='pending'
            ORDER BY id DESC LIMIT 20
        """)
        queries = c.fetchall()

    if not queries:
        print("\n✅ Koi pending query nahi hai!\n")
        return

    print(f"\n{'─'*60}")
    print(f"❓ PENDING CUSTOMER QUERIES ({len(queries)} found)")
    print(f"{'─'*60}")

    for q in queries:
        qid, fname, uname, uid, gname, qtext = q
        print(f"\n🆔 Query #{qid}")
        print(f"👤 Customer: {fname} ({uname})")
        print(f"🏠 Group: {gname}")
        print(f"💬 Query: {qtext}")
        print(f"{'─'*40}")

        reply = input("✏️  Apna reply type karo (ya SKIP likhke skip karo): ").strip()

        if reply.lower() == "skip":
            print("⏭️  Skipped!")
            continue

        success = await send_to_customer(uid, reply)
        if success:
            with sqlite3.connect(DB) as conn:
                conn.execute("UPDATE customer_queries SET status='replied', replied_at=? WHERE id=?",
                             (now(), qid))
            print(f"✅ Reply sent to {fname}!")
        else:
            print(f"❌ Failed to send. Check if customer has blocked you or privacy settings.")

# ═══════════════════════════════════════════
#  LIVE MODE - Real-time query detection
# ═══════════════════════════════════════════

@client.on(events.NewMessage)
async def live_query_handler(event):
    """Real-time mein group queries detect karo"""
    if event.is_private:
        return

    sender = await event.get_sender()
    if not sender or getattr(sender, "bot", False):
        return

    text = event.raw_text or ""
    if not text or not is_customer_query(text):
        return

    chat = await event.get_chat()
    gid = event.chat_id
    gname = getattr(chat, "title", "Unknown")
    uid = sender.id
    uname = f"@{sender.username}" if getattr(sender, "username", None) else ""
    fname = f"{getattr(sender, 'first_name', '')} {getattr(sender, 'last_name', '')}".strip()

    with sqlite3.connect(DB) as conn:
        conn.execute("""
            INSERT INTO customer_queries (user_id, username, full_name, group_id, group_name, query_text, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (uid, uname, fname, gid, gname, text[:500], now()))

    log.info(f"🔔 New query from {fname} in {gname}: {text[:60]}")

    # Auto reply with matching product
    products = get_matching_products(text)
    if products:
        reply = f"Hi {fname}! 👋\n\n"
        for p in products[:2]:
            reply += f"🛍️ **{p[0]}**\n{p[1][:200]}\n\n"
        reply += "Contact karo order ke liye! ✅"
        await event.reply(reply, parse_mode="markdown")


def get_matching_products(text):
    words = [w for w in text.lower().split() if len(w) > 3]
    results = []
    with sqlite3.connect(DB) as conn:
        c = conn.cursor()
        for word in words:
            c.execute("SELECT name, full_text FROM products WHERE LOWER(name) LIKE ? OR LOWER(full_text) LIKE ?",
                      (f"%{word}%", f"%{word}%"))
            results.extend(c.fetchall())
    seen = set()
    unique = []
    for r in results:
        if r[0] not in seen:
            seen.add(r[0])
            unique.append(r)
    return unique

# ═══════════════════════════════════════════
#  MAIN MENU
# ═══════════════════════════════════════════

async def main():
    init_db()
    print("\n🤖 Userbot starting... (Pehli baar phone number + OTP maangega)\n")

    await client.start()  # Phone se login

    me = await client.get_me()
    print(f"\n✅ Logged in as: {me.first_name} (+{me.phone})\n")

    while True:
        print("\n" + "═"*50)
        print("       🤖 USERBOT MENU")
        print("═"*50)
        print("1. 📦 Channel se products fetch karo")
        print("2. 🏠 Joined groups fetch karo")
        print("3. 🔍 Groups se customer queries fetch karo")
        print("4. 💬 Pending queries dekho & reply karo")
        print("5. 📨 Kisi bhi customer ko message bhejo")
        print("6. 🔴 Live mode (real-time queries detect karo)")
        print("7. ❌ Exit")
        print("─"*50)

        choice = input("Option choose karo (1-7): ").strip()

        if choice == "1":
            count = await fetch_channel_products()
            print(f"\n✅ {count} products saved to DB!")

        elif choice == "2":
            groups = await fetch_my_groups()
            print(f"\n✅ {len(groups)} groups found:")
            for g in groups[:10]:
                print(f"  🏠 {g['name']} | Members: {g['members']}")
            if len(groups) > 10:
                print(f"  ... aur {len(groups)-10} groups")

        elif choice == "3":
            n = await fetch_group_queries()
            print(f"\n✅ {n} new queries saved!")

        elif choice == "4":
            await reply_to_pending_queries_interactively()

        elif choice == "5":
            uid_input = input("Customer ka User ID ya username daalo: ").strip()
            msg_input = input("Message likhoo: ").strip()
            try:
                target = int(uid_input) if uid_input.isdigit() else uid_input
                await send_to_customer(target, msg_input)
            except Exception as e:
                print(f"❌ Error: {e}")

        elif choice == "6":
            print("\n🔴 LIVE MODE ON — Groups se real-time queries detect ho rahe hain...")
            print("Ctrl+C se band karo\n")
            await client.run_until_disconnected()

        elif choice == "7":
            print("\n👋 Bye!")
            break

        else:
            print("❌ Invalid option!")

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
