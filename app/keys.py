"""
API-key registry.

The server has two key tiers:

  - admin    : the single key in $WARMAP_API_KEY (env). Only the operator
               holds this. Mints/revokes uploader keys, can quarantine
               uploads.
  - uploader : keys minted via POST /admin/keys, each with a friendly
               `name` so attribution survives a spoofed client_id field.
               Stored on disk in <root>/keys/api_keys.json.

A request authenticates with `X-WarMap-Key: <hex>`. validate() returns a
dict describing the key (or None) so endpoints can record which key
performed which action.

Concurrency: a single global lock around load/save covers our small write
volume (mint = once per friend, usage update = once per upload batch).
"""

from __future__ import annotations

import json
import os
import secrets
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

_LOCK = threading.Lock()


@dataclass
class KeyRecord:
    key:        str
    name:       str
    tier:       str         # 'admin' | 'uploader'
    created_at: float
    last_used:  float       = 0.0
    uploads:    int         = 0
    enabled:    bool        = True
    note:       str         = ''


@dataclass
class Registry:
    keys:         list[KeyRecord] = field(default_factory=list)
    schema_ver:   int             = 1


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

class KeyStore:
    def __init__(self, path: Path, admin_key: str):
        self.path      = path
        self.admin_key = (admin_key or '').strip()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._reg = self._load()

    def _load(self) -> Registry:
        if not self.path.exists():
            return Registry()
        try:
            data = json.loads(self.path.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError):
            return Registry()
        keys = [KeyRecord(**k) for k in data.get('keys', [])]
        return Registry(keys=keys, schema_ver=data.get('schema_ver', 1))

    def _save_unlocked(self) -> None:
        tmp = self.path.with_suffix('.json.tmp')
        tmp.write_text(json.dumps({
            'schema_ver': self._reg.schema_ver,
            'keys':       [asdict(k) for k in self._reg.keys],
        }, indent=2), encoding='utf-8')
        os.replace(tmp, self.path)

    # ---- Public ---------------------------------------------------------
    def validate(self, presented: Optional[str]) -> Optional[KeyRecord]:
        """Return the matching key record (or None).  Admin key matches a
        synthetic record with tier='admin' so endpoints uniformly receive
        a KeyRecord."""
        if not presented:
            return None
        presented = presented.strip()
        if self.admin_key and presented == self.admin_key:
            return KeyRecord(
                key=presented, name='admin', tier='admin',
                created_at=0.0, last_used=time.time(),
                uploads=0, enabled=True,
            )
        with _LOCK:
            for k in self._reg.keys:
                if k.key == presented:
                    return k if k.enabled else None
        return None

    def list_uploader_keys(self) -> list[KeyRecord]:
        with _LOCK:
            return list(self._reg.keys)

    def mint(self, name: str, note: str = '') -> KeyRecord:
        name = (name or '').strip()
        if not name:
            raise ValueError('name required')
        with _LOCK:
            # Names should be unique; if a duplicate exists, return existing.
            for k in self._reg.keys:
                if k.name == name and k.enabled:
                    return k
            new = KeyRecord(
                key=secrets.token_hex(32),
                name=name,
                tier='uploader',
                created_at=time.time(),
                note=note,
            )
            self._reg.keys.append(new)
            self._save_unlocked()
            return new

    def set_enabled(self, name_or_key: str, enabled: bool) -> Optional[KeyRecord]:
        with _LOCK:
            for k in self._reg.keys:
                if k.name == name_or_key or k.key == name_or_key:
                    k.enabled = enabled
                    self._save_unlocked()
                    return k
        return None

    def remove(self, name_or_key: str) -> bool:
        with _LOCK:
            n = len(self._reg.keys)
            self._reg.keys = [k for k in self._reg.keys
                              if k.name != name_or_key and k.key != name_or_key]
            if len(self._reg.keys) != n:
                self._save_unlocked()
                return True
        return False

    def record_upload(self, presented: str, n_files: int = 1) -> None:
        if not presented:
            return
        with _LOCK:
            for k in self._reg.keys:
                if k.key == presented:
                    k.uploads   += n_files
                    k.last_used = time.time()
                    self._save_unlocked()
                    return
