"""Disk-backed uploads and integrity checks for P2P and channel files."""

import base64
import hashlib
import json
import os
import re
import threading
import time
import uuid

CHUNK_BYTES = 320
UPLOAD_CHUNK_BYTES = 256 * 1024
MAX_FILE_BYTES = 2 * 1024 * 1024 * 1024
MAX_ACTIVE_FILES = 16
TRANSFER_RE = re.compile(r"^[0-9a-f]{32}$")


class FileStore:
    """Keep incomplete files private and publish only completed uploads."""

    def __init__(self, database_path):
        self.directory = os.path.abspath(database_path) + ".files"
        os.makedirs(self.directory, mode=0o700, exist_ok=True)
        self._lock = threading.RLock()
        self._active = {}
        self._uploads = {}
        self._sending = {}
        for filename in os.listdir(self.directory):
            if filename.endswith(".part"):
                os.remove(os.path.join(self.directory, filename))

    @staticmethod
    def clean_name(value):
        if not isinstance(value, str):
            raise ValueError("file name is required")
        name = value.replace("\\", "/").split("/")[-1]
        name = re.sub(r"[^A-Za-z0-9._-]", "_", name)[:100]
        if not name or name in (".", ".."):
            raise ValueError("invalid file name")
        return name

    def _path(self, transfer_id, extension):
        if not isinstance(transfer_id, str) or not TRANSFER_RE.fullmatch(transfer_id):
            raise ValueError("invalid transfer ID")
        return os.path.join(self.directory, transfer_id + extension)

    @staticmethod
    def _checksum(path):
        digest = hashlib.sha256()
        with open(path, "rb") as content:
            for block in iter(lambda: content.read(65536), b""):
                digest.update(block)
        return digest.hexdigest()

    def _write_metadata(self, transfer_id, metadata):
        temporary = self._path(transfer_id, ".json.tmp")
        with open(temporary, "w", encoding="utf-8") as output:
            json.dump(metadata, output)
        os.replace(temporary, self._path(transfer_id, ".json"))

    def _read_metadata(self, transfer_id):
        path = self._path(transfer_id, ".json")
        if not os.path.isfile(path):
            return None
        with open(path, "r", encoding="utf-8") as source:
            return json.load(source)

    def _expire(self, transfers, max_age):
        now = time.monotonic()
        for transfer_id, entry in list(transfers.items()):
            if now - entry["updated"] <= max_age:
                continue
            try:
                os.remove(self._path(transfer_id, ".part"))
            except FileNotFoundError:
                pass
            del transfers[transfer_id]

    def _finish(self, transfer_id, sender, recipient, name, size, data_path,
                status="received", checksum=None):
        metadata = {"id": transfer_id, "sender": sender, "recipient": recipient,
                    "name": name, "size": size, "status": status,
                    "progress": 100, "sha256": checksum or self._checksum(data_path),
                    "created_at": int(time.time())}
        final = self._path(transfer_id, ".bin")
        os.replace(data_path, final)
        self._write_metadata(transfer_id, metadata)
        return metadata

    def start_upload(self, sender, recipient, name, size):
        """Create an ordered, disk-backed upload without loading the file in RAM."""
        name = self.clean_name(name)
        if type(size) is not int or not 0 <= size <= MAX_FILE_BYTES:
            raise ValueError("file exceeds the transfer size limit")
        with self._lock:
            self._expire(self._uploads, 1800)
            if len(self._uploads) >= MAX_ACTIVE_FILES:
                raise ValueError("too many active uploads")
            transfer_id = uuid.uuid4().hex
            with open(self._path(transfer_id, ".part"), "wb"):
                pass
            self._uploads[transfer_id] = {"sender": sender, "recipient": recipient,
                                          "name": name, "size": size,
                                          "written": 0, "updated": time.monotonic()}
            return {"id": transfer_id, "name": name, "size": size}

    def append_upload(self, transfer_id, sender, recipient, index, encoded):
        if type(index) is not int or index < 0 or not isinstance(encoded, str):
            raise ValueError("invalid upload chunk")
        if len(encoded) > (UPLOAD_CHUNK_BYTES * 4 // 3 + 4):
            raise ValueError("upload chunk too large")
        try:
            data = base64.b64decode(encoded, validate=True)
        except (ValueError, TypeError) as exc:
            raise ValueError("invalid upload encoding") from exc
        if not data or len(data) > UPLOAD_CHUNK_BYTES:
            raise ValueError("invalid upload chunk length")
        with self._lock:
            entry = self._uploads.get(transfer_id)
            if entry is None or (entry["sender"], entry["recipient"]) != (sender, recipient):
                raise ValueError("upload not found")
            if index * UPLOAD_CHUNK_BYTES != entry["written"]:
                raise ValueError("out-of-order upload chunk")
            if entry["written"] + len(data) > entry["size"]:
                raise ValueError("upload exceeds announced size")
            with open(self._path(transfer_id, ".part"), "ab") as output:
                output.write(data)
            entry["written"] += len(data)
            entry["updated"] = time.monotonic()
            return entry["written"]

    def finish_upload(self, transfer_id, sender, recipient, status="queued"):
        with self._lock:
            entry = self._uploads.get(transfer_id)
            if entry is None or (entry["sender"], entry["recipient"]) != (sender, recipient):
                raise ValueError("upload not found")
            if entry["written"] != entry["size"]:
                raise ValueError("upload is incomplete")
            part = self._path(transfer_id, ".part")
            checksum = self._checksum(part)
            metadata = self._finish(transfer_id, sender, recipient,
                                    entry["name"], entry["size"], part,
                                    status=status, checksum=checksum)
            del self._uploads[transfer_id]
            return metadata

    def update_status(self, transfer_id, status):
        """Record a local sending state; not a remote delivery receipt."""
        with self._lock:
            metadata = self._read_metadata(transfer_id)
            if metadata is None:
                raise FileNotFoundError(self._path(transfer_id, ".json"))
            metadata["status"] = status
            self._write_metadata(transfer_id, metadata)
            self._sending.pop(transfer_id, None)

    def set_progress(self, transfer_id, percentage):
        with self._lock:
            self._sending[transfer_id] = percentage

    def save_sent(self, transfer_id, sender, recipient, name, contents):
        name = self.clean_name(name)
        if not isinstance(contents, bytes) or len(contents) > MAX_FILE_BYTES:
            raise ValueError("file is too large")
        with self._lock:
            part = self._path(transfer_id, ".part")
            with open(part, "wb") as output:
                output.write(contents)
            return self._finish(transfer_id, sender, recipient, name,
                                len(contents), part, status="queued")

    def receive_chunk(self, envelope):
        """Return true only after all received P2P chunks pass SHA-256."""
        try:
            chunk = json.loads(envelope["body"])
            transfer_id = chunk["transfer"]
            name = self.clean_name(chunk["name"])
            size = chunk["size"]
            total = chunk["total"]
            index = chunk["index"]
            checksum = chunk["sha256"]
            data = base64.b64decode(chunk["data"], validate=True)
        except (KeyError, ValueError, TypeError) as exc:
            raise ValueError("invalid file chunk") from exc
        if not isinstance(transfer_id, str) or not TRANSFER_RE.fullmatch(transfer_id):
            raise ValueError("invalid file ID")
        if (type(size) is not int or not 0 <= size <= MAX_FILE_BYTES or
                type(total) is not int or total != max(1, (size + CHUNK_BYTES - 1) // CHUNK_BYTES) or
                type(index) is not int or not 0 <= index < total or
                not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum)):
            raise ValueError("invalid file metadata")
        expected = min(CHUNK_BYTES, max(0, size - index * CHUNK_BYTES))
        if len(data) != expected:
            raise ValueError("invalid chunk length")
        with self._lock:
            if os.path.exists(self._path(transfer_id, ".json")):
                return False
            self._expire(self._active, 600)
            active = self._active.get(transfer_id)
            identity = (envelope["from"], envelope["to"], name, size, total, checksum)
            if active is None:
                if len(self._active) >= MAX_ACTIVE_FILES:
                    raise ValueError("too many active transfers")
                active = {"identity": identity, "bitmap": bytearray((total + 7) // 8),
                          "count": 0, "updated": time.monotonic()}
                self._active[transfer_id] = active
                with open(self._path(transfer_id, ".part"), "wb"):
                    pass
            elif active["identity"] != identity:
                raise ValueError("inconsistent file transfer metadata")
            active["updated"] = time.monotonic()
            mask = 1 << (index % 8)
            if active["bitmap"][index // 8] & mask:
                return False
            with open(self._path(transfer_id, ".part"), "r+b") as output:
                output.seek(index * CHUNK_BYTES)
                output.write(data)
            active["bitmap"][index // 8] |= mask
            active["count"] += 1
            if active["count"] != total:
                return False
            part = self._path(transfer_id, ".part")
            del self._active[transfer_id]
            if self._checksum(part) != checksum or os.path.getsize(part) != size:
                os.remove(part)
                raise ValueError("file checksum mismatch")
            self._finish(transfer_id, envelope["from"], envelope["to"],
                         name, size, part, checksum=checksum)
            return True

    def download_path(self, transfer_id):
        return self._path(transfer_id, ".bin")

    def get(self, transfer_id, viewer):
        metadata = self._read_metadata(transfer_id)
        if metadata is None:
            return None
        if viewer.casefold() not in (metadata["sender"].casefold(),
                                     metadata["recipient"].casefold()):
            return None
        return metadata

    def get_channel(self, transfer_id, channel_id):
        metadata = self._read_metadata(transfer_id)
        return metadata if metadata and metadata["recipient"] == channel_id else None

    def iter_send_chunks(self, transfer_id, size):
        """Read P2P chunks lazily; never buffer the complete file in memory."""
        total = max(1, (size + CHUNK_BYTES - 1) // CHUNK_BYTES)
        with open(self.download_path(transfer_id), "rb") as source:
            for index in range(total):
                yield index, total, source.read(CHUNK_BYTES)

    def read_chunk(self, transfer_id, index):
        if type(index) is not int or index < 0:
            raise ValueError("invalid chunk index")
        with open(self.download_path(transfer_id), "rb") as content:
            content.seek(index * UPLOAD_CHUNK_BYTES)
            return content.read(UPLOAD_CHUNK_BYTES)

    def list_channel(self, channel_id):
        rows = []
        with self._lock:
            for filename in os.listdir(self.directory):
                if not filename.endswith(".json"):
                    continue
                metadata = self.get_channel(filename[:-5], channel_id)
                if metadata is not None:
                    rows.append(metadata)
        return sorted(rows, key=lambda row: (row["created_at"], row["id"]))

    def list(self, viewer, peer):
        rows = []
        with self._lock:
            for filename in os.listdir(self.directory):
                if not filename.endswith(".json"):
                    continue
                metadata = self.get(filename[:-5], viewer)
                if metadata and peer.casefold() in (metadata["sender"].casefold(),
                                                      metadata["recipient"].casefold()):
                    if metadata["id"] in self._sending:
                        metadata["progress"] = self._sending[metadata["id"]]
                    rows.append(metadata)
            for transfer_id, entry in self._active.items():
                sender, recipient, name, size, total, _checksum = entry["identity"]
                if (viewer.casefold() != recipient.casefold() or
                        peer.casefold() != sender.casefold()):
                    continue
                rows.append({
                    "id": transfer_id, "name": name, "size": size,
                    "sender": sender, "recipient": recipient,
                    "status": "receiving",
                    "progress": int(entry["count"] * 100 / total),
                    "created_at": 0,
                })
        return sorted(rows, key=lambda row: (row["created_at"], row["id"]))
