import os
from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма
# Telegram
TG_TOKEN = os.environ.get('TG_TOKEN')
TG_CHAT_ID = os.environ.get('TG_CHAT_ID')

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    hook = TelegramHook(token=TG_TOKEN, chat_id=TG_CHAT_ID)
    dag = context['dag'].dag_id
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': TG_CHAT_ID,
        'text': message
    }) # отправление сообщения 

def send_telegram_failure_message(context):
    hook = TelegramHook(token=TG_TOKEN, chat_id=TG_CHAT_ID)
    dag = context['dag']
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']

    message = f'Исполнение DAG {dag} с id={run_id} провалилось! Task:{task_instance_key_str}!' # определение текста сообщения
    hook.send_message({
        'chat_id': TG_CHAT_ID,
        'text': message})