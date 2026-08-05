from ...ext_utils.status_utils import (
    MirrorStatus,
    get_readable_file_size,
    get_readable_time,
)

class AllDebridStatus:
    def __init__(self, listener):
        self.listener = listener
        self._gid = listener.mid
        self.tool = "alldebrid"
        self._status = {}

    def update(self, status):
        self._status = status

    def gid(self):
        return str(self.listener.mid)

    def progress_raw(self):
        try:
            size = self._status.get("size", 0)
            downloaded = self._status.get("downloaded", 0)
            if size > 0:
                return (downloaded / size) * 100
        except:
            pass
        return 0

    def progress(self):
        return f"{round(self.progress_raw(), 2)}%"

    def speed(self):
        return f"{get_readable_file_size(self._status.get('downloadSpeed', 0))}/s"

    def name(self):
        return self._status.get("filename") or self.listener.name or "Unknown"

    def size(self):
        return get_readable_file_size(self._status.get("size", 0))

    def eta(self):
        try:
            speed = self._status.get("downloadSpeed", 0)
            size = self._status.get("size", 0)
            downloaded = self._status.get("downloaded", 0)
            if speed > 0 and size > 0:
                seconds = (size - downloaded) / speed
                return get_readable_time(seconds)
        except:
            pass
        return "-"

    def status(self):
        return MirrorStatus.STATUS_DOWNLOAD

    def processed_bytes(self):
        return get_readable_file_size(self._status.get("downloaded", 0))

    def task(self):
        return self

    async def cancel_task(self):
        self.listener.is_cancelled = True
