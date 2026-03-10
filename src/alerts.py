import requests

def send_alert(message):
    token = "8379434413:AAFsUGCoktuan8pW0XlW515nC6Qndb5h-bM"
    chat_id = "5203055547"
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    try:
        response = requests.post(url, data=payload)
        return response.ok
    except Exception as e:
        print(f"Erro ao enviar Telegram: {e}")
        return False