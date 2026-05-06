from PIL import Image
from aioshutil import rmtree
from asyncio import sleep, gather, Queue
from logging import getLogger
from natsort import natsorted
from os import walk, path as ospath
from time import time
from re import match as re_match, sub as re_sub
from pyrogram.errors import FloodWait, RPCError, FloodPremiumWait, BadRequest
from pyrogram.types import (
    InputMediaVideo,
    InputMediaDocument,
    InputMediaPhoto,
)
from aiofiles.os import (
    remove,
    path as aiopath,
    rename,
)
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
    RetryError,
)

from ... import intervals
from ...core.config_manager import Config
from ...core.telegram_manager import TgClient
from ..ext_utils.bot_utils import sync_to_async
from ..ext_utils.files_utils import is_archive, get_base_name
from ..telegram_helper.message_utils import delete_message
from ..ext_utils.media_utils import (
    get_media_info,
    get_document_type,
    get_video_thumbnail,
    get_audio_thumbnail,
    get_multiple_frames_thumbnail,
)

LOGGER = getLogger(__name__)


class TelegramUploader:
    def __init__(self, listener, path):
        self._last_uploaded = 0
        self._processed_bytes = 0
        self._listener = listener
        self._path = path
        self._start_time = time()
        self._total_files = 0
        self._thumb = self._listener.thumb or f"thumbnails/{listener.user_id}.jpg"
        self._msgs_dict = {}
        self._corrupted = 0
        self._is_corrupted = False
        self._media_dict = {"videos": {}, "documents": {}}
        self._last_msg_in_group = False
        self._up_path = ""
        self._lprefix = ""
        self._media_group = False
        self._is_private = False
        self._sent_msg = None
        self._user_session = self._listener.user_transmission
        self._error = ""
        self._base_msg = None
        self._files_links = False
        self._bot_index = 0

    # _upload_progress was removed in favor of closures for parallel tracking

    async def _user_settings(self):
        self._media_group = self._listener.user_dict.get("MEDIA_GROUP", False) or (
            Config.MEDIA_GROUP
            if "MEDIA_GROUP" not in self._listener.user_dict
            else False
        )
        self._lprefix = self._listener.user_dict.get("LEECH_FILENAME_PREFIX") or (
            Config.LEECH_FILENAME_PREFIX
            if "LEECH_FILENAME_PREFIX" not in self._listener.user_dict
            else ""
        )
        self._files_links = self._listener.user_dict.get("FILES_LINKS", False) or (
            Config.FILES_LINKS
            if "FILES_LINKS" not in self._listener.user_dict
            else False
        )
        if self._thumb != "none" and not await aiopath.exists(self._thumb):
            self._thumb = None

    async def _msg_to_reply(self):
        if self._listener.up_dest:
            msg = (
                self._listener.message.link
                if self._listener.is_super_chat
                else self._listener.message.text.lstrip("/")
            )
            try:
                if self._user_session:
                    self._sent_msg = await TgClient.user.send_message(
                        chat_id=self._listener.up_dest,
                        text=msg,
                        message_thread_id=self._listener.chat_thread_id,
                        disable_notification=True,
                    )
                else:
                    self._sent_msg = await self._listener.client.send_message(
                        chat_id=self._listener.up_dest,
                        text=msg,
                        message_thread_id=self._listener.chat_thread_id,
                        disable_notification=True,
                    )
                    self._is_private = self._sent_msg.chat.type.name == "PRIVATE"
            except Exception as e:
                await self._listener.on_upload_error(str(e))
                return False
            finally:
                self._base_msg = self._sent_msg
        elif self._user_session:
            self._sent_msg = await TgClient.user.get_messages(
                chat_id=self._listener.message.chat.id, message_ids=self._listener.mid
            )
            if self._sent_msg is None:
                self._sent_msg = await TgClient.user.send_message(
                    chat_id=self._listener.message.chat.id,
                    text="Deleted Cmd Message! Don't delete the cmd message again!",
                    disable_notification=True,
                )
        else:
            self._sent_msg = self._listener.message
        return True

    async def _prepare_file(self, file_, dirpath, f_path):
        if self._lprefix:
            cap_mono = f"{self._lprefix} <code>{file_}</code>"
            lprefix_clean = re_sub("<.*?>", "", self._lprefix)
            new_path = ospath.join(dirpath, f"{lprefix_clean} {file_}")
            await rename(f_path, new_path)
            f_path = new_path
        else:
            cap_mono = f"<code>{file_}</code>"
        if len(file_) > 60:
            if is_archive(file_):
                name = get_base_name(file_)
                ext = file_.split(name, 1)[1]
            elif match := re_match(r".+(?=\..+\.0*\d+$)|.+(?=\.part\d+\..+$)", file_):
                name = match.group(0)
                ext = file_.split(name, 1)[1]
            elif len(fsplit := ospath.splitext(file_)) > 1:
                name = fsplit[0]
                ext = fsplit[1]
            else:
                name = file_
                ext = ""
            extn = len(ext)
            remain = 60 - extn
            name = name[:remain]
            new_path = ospath.join(dirpath, f"{name}{ext}")
            await rename(f_path, new_path)
            f_path = new_path
        return cap_mono, f_path

    def _get_input_media(self, subkey, key):
        rlist = []
        for msg in self._media_dict[key][subkey]:
            if key == "videos":
                input_media = InputMediaVideo(
                    media=msg.video.file_id, caption=msg.caption
                )
            else:
                input_media = InputMediaDocument(
                    media=msg.document.file_id, caption=msg.caption
                )
            rlist.append(input_media)
        return rlist

    async def _send_screenshots(self, dirpath, outputs):
        inputs = [
            InputMediaPhoto(ospath.join(dirpath, p), p.rsplit("/", 1)[-1])
            for p in outputs
        ]
        for i in range(0, len(inputs), 10):
            batch = inputs[i : i + 10]
            self._sent_msg = (
                await self._sent_msg.reply_media_group(
                    media=batch,
                    disable_notification=True,
                )
            )[-1]

    async def _send_media_group(self, subkey, key, msgs):
        for index, msg in enumerate(msgs):
            if self._listener.hybrid_leech or not self._user_session:
                msgs[index] = await self._listener.client.get_messages(
                    chat_id=msg[0], message_ids=msg[1]
                )
            else:
                msgs[index] = await TgClient.user.get_messages(
                    chat_id=msg[0], message_ids=msg[1]
                )
        msgs_list = await msgs[0].reply_to_message.reply_media_group(
            media=self._get_input_media(subkey, key),
            disable_notification=True,
        )
        for msg in msgs:
            if msg.link in self._msgs_dict:
                del self._msgs_dict[msg.link]
            await delete_message(msg)
        del self._media_dict[key][subkey]
        if self._files_links and (
            self._listener.is_super_chat or self._listener.up_dest
        ):
            for m in msgs_list:
                self._msgs_dict[m.link] = m.caption
        self._sent_msg = msgs_list[-1]
        if self._base_msg:
            await delete_message(self._base_msg)
            self._base_msg = None

    async def _upload_task(self, cap_mono, file_, f_path, base_msg, queue):
        if self._listener.is_cancelled:
            return
            
        last_uploaded = [0]
        async def _progress(current, _):
            if self._listener.is_cancelled:
                if self._user_session:
                    TgClient.user.stop_transmission()
                else:
                    self._listener.client.stop_transmission()
                    for b in TgClient.extra_bots:
                        b.stop_transmission()
            chunk_size = current - last_uploaded[0]
            last_uploaded[0] = current
            self._processed_bytes += chunk_size

        current_client = await queue.get()
        try:
            sent_msg = await self._upload_file(cap_mono, file_, f_path, base_msg, current_client, progress_cb=_progress)
            if sent_msg and sent_msg.media_group_id:
                for ch, ch_data in list(self._listener.clone_dump_chats.items()):
                    try:
                        res = await TgClient.bot.copy_message(
                            chat_id=ch,
                            from_chat_id=sent_msg.chat.id,
                            message_id=sent_msg.id,
                            message_thread_id=ch_data["thread_id"],
                            disable_notification=True,
                            reply_to_message_id=ch_data["last_sent_msg"],
                        )
                        self._listener.clone_dump_chats[ch]["last_sent_msg"] = res.id
                    except Exception as e:
                        LOGGER.error(f"Can't forward message to clone dump chat: {ch}. Error: {e}")
            if (
                self._files_links
                and sent_msg
                and (self._listener.is_super_chat or self._listener.up_dest)
                and not self._is_private
            ):
                self._msgs_dict[sent_msg.link] = file_
        except Exception as err:
            if isinstance(err, RetryError):
                LOGGER.info(f"Total Attempts: {err.last_attempt.attempt_number}")
                err = err.last_attempt.exception()
            LOGGER.error(f"{err}. Path: {f_path}")
            self._error += f"{err}\n"
            self._corrupted += 1
        finally:
            queue.put_nowait(current_client)
            if not self._listener.is_cancelled and await aiopath.exists(f_path):
                await remove(f_path)

    async def upload(self):
        await self._user_settings()
        res = await self._msg_to_reply()
        if not res:
            return
            
        bots_pool = [TgClient.user] if self._user_session else [self._listener.client] + TgClient.extra_bots
        bots_queue = Queue()
        for bot in bots_pool:
            bots_queue.put_nowait(bot)
            
        tasks = []
        for dirpath, _, files in natsorted(await sync_to_async(walk, self._path)):
            if dirpath.strip().endswith("/yt-dlp-thumb"):
                continue
            if dirpath.strip().endswith("_mltbss"):
                await self._send_screenshots(dirpath, files)
                await rmtree(dirpath, ignore_errors=True)
                continue
            for file_ in natsorted(files):
                f_path = ospath.join(dirpath, file_)
                if not await aiopath.exists(f_path):
                    if intervals["stopAll"]:
                        return
                    LOGGER.error(f"{f_path} not exists! Continue uploading!")
                    continue
                try:
                    f_size = await aiopath.getsize(f_path)
                    if f_size == 0:
                        LOGGER.error(f"{f_path} size is zero, telegram don't upload zero size files")
                        self._corrupted += 1
                        self._total_files += 1
                        continue
                    if self._listener.is_cancelled:
                        return
                    cap_mono, f_path = await self._prepare_file(file_, dirpath, f_path)
                    self._total_files += 1
                    tasks.append(self._upload_task(cap_mono, file_, f_path, self._sent_msg, bots_queue))
                except Exception as e:
                    LOGGER.error(e)
                    
        if tasks:
            await gather(*tasks)
            
        for key, value in list(self._media_dict.items()):
            for subkey, msgs in list(value.items()):
                if len(msgs) > 1:
                    try:
                        await self._send_media_group(subkey, key, msgs)
                    except Exception as e:
                        LOGGER.info(f"While sending media group at the end of task. Error: {e}")
                        
        if self._base_msg:
            await delete_message(self._base_msg)
            self._base_msg = None
            
        if self._listener.is_cancelled:
            return
        if self._total_files == 0:
            await self._listener.on_upload_error(
                "No files to upload. In case you have filled EXCLUDED/INCLUDED EXTENSIONS, then check if all files have those extensions or not."
            )
            return
        if self._total_files <= self._corrupted:
            await self._listener.on_upload_error(
                f"Files Corrupted or unable to upload. {self._error or 'Check logs!'}"
            )
            return
        LOGGER.info(f"Leech Completed: {self._listener.name}")
        await self._listener.on_upload_complete(
            None, self._msgs_dict, self._total_files, self._corrupted
        )
        return

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=8),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(Exception),
    )
    async def _upload_file(self, cap_mono, file, up_path, base_msg, current_client, force_document=False, progress_cb=None):
        thumb = self._thumb
        if thumb is not None and not await aiopath.exists(thumb) and thumb != "none":
            thumb = None
            
        try:
            is_video, is_audio, is_image = await get_document_type(up_path)

            if not is_image and thumb is None:
                file_name = ospath.splitext(file)[0]
                thumb_path = f"{self._path}/yt-dlp-thumb/{file_name}.jpg"
                if await aiopath.isfile(thumb_path):
                    thumb = thumb_path
                elif await aiopath.isfile(thumb_path.replace("/yt-dlp-thumb", "")):
                    thumb = thumb_path.replace("/yt-dlp-thumb", "")
                elif is_audio and not is_video:
                    thumb = await get_audio_thumbnail(up_path)

            if (
                self._listener.as_doc
                or force_document
                or (not is_video and not is_audio and not is_image)
            ):
                key = "documents"
                if is_video and thumb is None:
                    thumb = await get_video_thumbnail(up_path, None)

                if self._listener.is_cancelled:
                    return None
                if thumb == "none":
                    thumb = None
                sent_msg = await current_client.send_document(
                    chat_id=base_msg.chat.id,
                    reply_to_message_id=base_msg.id,
                    document=up_path,
                    thumb=thumb,
                    caption=cap_mono,
                    force_document=True,
                    disable_notification=True,
                    progress=progress_cb,
                )
            elif is_video:
                key = "videos"
                duration = (await get_media_info(up_path))[0]
                if thumb is None and self._listener.thumbnail_layout:
                    thumb = await get_multiple_frames_thumbnail(
                        up_path,
                        self._listener.thumbnail_layout,
                        self._listener.screen_shots,
                    )
                if thumb is None:
                    thumb = await get_video_thumbnail(up_path, duration)
                if thumb is not None and thumb != "none":
                    with Image.open(thumb) as img:
                        width, height = img.size
                else:
                    width = 480
                    height = 320
                if self._listener.is_cancelled:
                    return None
                if thumb == "none":
                    thumb = None
                sent_msg = await current_client.send_video(
                    chat_id=base_msg.chat.id,
                    reply_to_message_id=base_msg.id,
                    video=up_path,
                    caption=cap_mono,
                    duration=duration,
                    width=width,
                    height=height,
                    thumb=thumb,
                    supports_streaming=True,
                    disable_notification=True,
                    progress=progress_cb,
                )
            elif is_audio:
                key = "audios"
                duration, artist, title = await get_media_info(up_path)
                if self._listener.is_cancelled:
                    return None
                if thumb == "none":
                    thumb = None
                sent_msg = await current_client.send_audio(
                    chat_id=base_msg.chat.id,
                    reply_to_message_id=base_msg.id,
                    audio=up_path,
                    caption=cap_mono,
                    duration=duration,
                    performer=artist,
                    title=title,
                    thumb=thumb,
                    disable_notification=True,
                    progress=progress_cb,
                )
            else:
                key = "photos"
                if self._listener.is_cancelled:
                    return None
                sent_msg = await current_client.send_photo(
                    chat_id=base_msg.chat.id,
                    reply_to_message_id=base_msg.id,
                    photo=up_path,
                    caption=cap_mono,
                    disable_notification=True,
                    progress=progress_cb,
                )

            if (
                not self._listener.is_cancelled
                and self._media_group
                and (sent_msg.video or sent_msg.document)
            ):
                key = "documents" if sent_msg.document else "videos"
                if match := re_match(r".+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)", up_path):

                    pname = match.group(0)
                    if pname in self._media_dict[key].keys():
                        self._media_dict[key][pname].append(
                            [sent_msg.chat.id, sent_msg.id]
                        )
                    else:
                        self._media_dict[key][pname] = [
                            [sent_msg.chat.id, sent_msg.id]
                        ]
            if (
                self._thumb is None
                and thumb is not None
                and await aiopath.exists(thumb)
            ):
                await remove(thumb)
            return sent_msg
        except (FloodWait, FloodPremiumWait) as f:
            LOGGER.warning(str(f))
            await sleep(f.value * 1.3)
            if (
                self._thumb is None
                and thumb is not None
                and await aiopath.exists(thumb)
            ):
                await remove(thumb)
            return await self._upload_file(cap_mono, file, up_path, base_msg, current_client, force_document, progress_cb)
        except Exception as err:
            if (
                self._thumb is None
                and thumb is not None
                and await aiopath.exists(thumb)
            ):
                await remove(thumb)
            err_type = "RPCError: " if isinstance(err, RPCError) else ""
            LOGGER.error(f"{err_type}{err}. Path: {up_path}")
            if isinstance(err, BadRequest) and key != "documents":
                LOGGER.error(f"Retrying As Document. Path: {up_path}")
                return await self._upload_file(cap_mono, file, up_path, base_msg, current_client, True, progress_cb)
            raise err

    @property
    def speed(self):
        try:
            return self._processed_bytes / (time() - self._start_time)
        except:
            return 0

    @property
    def processed_bytes(self):
        return self._processed_bytes

    async def cancel_task(self):
        self._listener.is_cancelled = True
        LOGGER.info(f"Cancelling Upload: {self._listener.name}")
        await self._listener.on_upload_error("your upload has been stopped!")
