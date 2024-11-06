import telebot
import os
from telebot import types
from central_cinema import calculate_metrics
from datetime import datetime

# Настройки подключения и токена
token = 'token'
pg_user = 'user'
pg_password = 'password'
pg_host = 'host'
# @cinema_online_info_bot - имя бота в тг

bot = telebot.TeleBot(token)


@bot.message_handler(commands=['start'])
def start(message):

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    button = types.KeyboardButton("Запросить отчёт")
    markup.add(button)
    bot.send_message(message.chat.id, "Нажмите кнопку, чтобы запросить отчёт.", reply_markup=markup).message_id
    bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)

@bot.message_handler(func=lambda message: message.text == "Запросить отчёт")
def send_report(message):


    chat_id = message.chat.id


    today = datetime.now()

    report_message = calculate_metrics(pg_user, pg_password, pg_host, today)
    bot.send_message(chat_id, report_message)

# Запуск бота
bot.polling(none_stop=True)
