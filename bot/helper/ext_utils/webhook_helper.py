import aiohttp
from bot import LOGGER
from bot.core.config_manager import Config

async def send_webhook(event_type: str, data: dict):
    webhook_url = Config.WEBHOOK_URL
    if not webhook_url:
        return
        
    payload = {
        "event": event_type,
        "data": data
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(webhook_url, json=payload, timeout=10) as response:
                if response.status not in (200, 201, 204):
                    LOGGER.warning(f"Webhook failed with status {response.status}: {await response.text()}")
                else:
                    LOGGER.info(f"Webhook {event_type} sent successfully to {webhook_url}")
    except Exception as e:
        LOGGER.error(f"Error sending webhook for event {event_type}: {str(e)}")
