from pyrogram.handlers import MessageHandler
from pyrogram.filters import command

from .. import LOGGER
from ..core.telegram_manager import TgClient
from ..helper.telegram_helper.bot_commands import BotCommands
from ..helper.telegram_helper.filters import CustomFilters
from ..helper.telegram_helper.message_utils import send_message
from ..helper.ext_utils.bot_utils import new_task
from ..core.jdownloader_booter import jdownloader

@new_task
async def clear_jd_queue(_, message):
    msg = await send_message(message, "Connecting to internal JDownloader server...")
    try:
        links = await jdownloader.device.downloads.query_links([{"finished": True}])
        bad_links = [l["uuid"] for l in links if not l.get("finished", False)]
        
        if bad_links:
            await msg.edit(f"Found {len(bad_links)} stuck links! Deleting them now...")
            await jdownloader.device.downloads.remove_links(link_ids=bad_links)
            await msg.edit(f"✅ SUCCESSFULLY PURGED {len(bad_links)} STUCK LINKS!\nJDownloader will now mark the package as finished and Anasty will begin uploading!")
        else:
            await msg.edit("✅ No unfinished links found!")
    except Exception as e:
        await msg.edit(f"❌ Error: {e}")
