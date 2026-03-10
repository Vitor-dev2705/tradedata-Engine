import requests

def send_alert(message):
    token = "sei token"
    chat_id = "seu id"
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