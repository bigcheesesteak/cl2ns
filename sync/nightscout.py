"""cl2ns - Nightscout API Uploader

Clean, lightweight async uploader for Nightscout v1 REST API.
"""

import hashlib
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import aiofiles
import httpx

_LOGGER = logging.getLogger("cl2ns.nightscout")

USER_AGENT = "cl2ns-sync-engine/1.0"
DEDUP_RETENTION_HOURS = 25


class NightscoutError(Exception):
    """Base exception for Nightscout API errors."""
    pass


class NightscoutUploader:
    """Nightscout REST API uploader with fingerprint deduplication."""

    def __init__(self, url: str, api_secret: str, data_dir: str = "/data"):
        self.url = url.rstrip("/")
        self.api_secret = api_secret
        self.hashed_secret = hashlib.sha1(api_secret.encode("utf-8")).hexdigest()
        self.data_dir = data_dir
        self.cache_file_path = os.path.join(data_dir, "ns_dedup_cache.json")

        self.fingerprints: dict[str, str] = {}
        self._cache_loaded = False
        self._http_client = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(
                headers={
                    "API-SECRET": self.hashed_secret,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "User-Agent": USER_AGENT,
                },
                timeout=30.0,
                follow_redirects=True,
            )
        return self._http_client

    async def close(self):
        """Close HTTP client session."""
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()
            self._http_client = None

    async def load_cache(self):
        """Load persistent deduplication cache."""
        if self._cache_loaded:
            return
        if not os.path.exists(self.cache_file_path):
            self._cache_loaded = True
            return

        try:
            async with aiofiles.open(self.cache_file_path, "r") as f:
                self.fingerprints = json.loads(await f.read())
            _LOGGER.debug(f"Loaded {len(self.fingerprints)} entries from dedup cache.")
        except Exception as e:
            _LOGGER.warning(f"Could not load dedup cache, starting fresh: {e}")
            self.fingerprints = {}
        self._cache_loaded = True

    async def save_cache(self):
        """Atomically persist deduplication cache."""
        os.makedirs(self.data_dir, exist_ok=True)
        tmp_file = self.cache_file_path + ".tmp"
        try:
            async with aiofiles.open(tmp_file, "w") as f:
                await f.write(json.dumps(self.fingerprints))
            os.replace(tmp_file, self.cache_file_path)
        except Exception as e:
            _LOGGER.warning(f"Failed saving dedup cache: {e}")

    def purge_expired_cache(self):
        """Purge fingerprints older than retention window."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=DEDUP_RETENTION_HOURS)
        fresh_map = {}
        for fp, ts_iso in self.fingerprints.items():
            try:
                if datetime.fromisoformat(ts_iso) > cutoff:
                    fresh_map[fp] = ts_iso
            except Exception:
                pass
        self.fingerprints = fresh_map

    def _compute_fingerprint(self, entry: dict, endpoint: str) -> str:
        """Calculate SHA-256 hash of key entry attributes."""
        if endpoint == "devicestatus":
            return None  # Device status updates are real-time, no dedup needed

        if endpoint == "treatments":
            fields = (
                str(entry.get("eventType", "")),
                str(entry.get("created_at", "")),
                str(entry.get("carbs", "")),
                str(entry.get("insulin", "")),
                str(entry.get("notes", "")),
            )
        elif endpoint == "entries":
            fields = (
                str(entry.get("type", "")),
                str(entry.get("dateString", "")),
                str(entry.get("sgv", "")),
            )
        else:
            return None

        raw = "|".join(fields)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    async def test_connection(self) -> bool:
        """Verify reachability of Nightscout instance."""
        client = await self._get_client()
        try:
            resp = await client.get(f"{self.url}/api/v1/devicestatus.json?count=1")
            return resp.status_code == 200
        except Exception as e:
            _LOGGER.error(f"Nightscout reachability test failed: {e}")
            return False

    async def _post_batch(self, endpoint: str, items: list) -> tuple[int, int]:
        """Post array of records to Nightscout endpoint with deduplication."""
        if not items:
            return 0, 0

        client = await self._get_client()
        target_url = f"{self.url}/api/v1/{endpoint}"
        to_upload = []

        for item in items:
            fp = self._compute_fingerprint(item, endpoint)
            if fp and fp in self.fingerprints:
                continue
            to_upload.append((item, fp))

        skipped = len(items) - len(to_upload)
        if not to_upload:
            return 0, skipped

        uploaded = 0
        now_iso = datetime.now(timezone.utc).isoformat()

        for item, fp in to_upload:
            resp = await client.post(target_url, json=item)
            if resp.status_code in (200, 201):
                uploaded += 1
                if fp:
                    self.fingerprints[fp] = now_iso
            else:
                _LOGGER.error(f"Nightscout {endpoint} upload error HTTP {resp.status_code}: {resp.text}")

        return uploaded, skipped

    @staticmethod
    def _parse_timestamp(iso_str: str, target_tz: ZoneInfo) -> tuple[int, str]:
        """Convert ISO date string to epoch ms and ISO string with target timezone."""
        clean = re.sub(r"\.\d{3}Z$", "+00:00", iso_str)
        dt = datetime.fromisoformat(clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(target_tz)
        epoch_ms = int(dt.timestamp() * 1000)
        return epoch_ms, dt.isoformat()

    def _build_sgv_entries(self, sgs: list, tz: ZoneInfo) -> list:
        """Transform Carelink SGS array into Nightscout SGV entries."""
        valid_sgs = [s for s in sgs if s.get("sensorState") == "NO_ERROR_MESSAGE"]
        entries = []

        for idx, sg in enumerate(valid_sgs):
            epoch_ms, date_str = self._parse_timestamp(sg["timestamp"], tz)
            trend = "Flat"
            delta = 0

            if idx > 0 and valid_sgs[idx - 1].get("sg", 0) > 0 and sg.get("sg", 0) > 0:
                diff = sg["sg"] - valid_sgs[idx - 1]["sg"]
                delta = diff
                if diff == 0:
                    trend = "Flat"
                elif diff < -30:
                    trend = "TripleDown"
                elif diff < -15:
                    trend = "DoubleDown"
                elif diff < -5:
                    trend = "SingleDown"
                elif diff < 0:
                    trend = "FortyFiveDown"
                elif diff > 30:
                    trend = "TripleUp"
                elif diff > 15:
                    trend = "DoubleUp"
                elif diff > 5:
                    trend = "SingleUp"
                elif diff > 0:
                    trend = "FortyFiveUp"

            entries.append({
                "device": USER_AGENT,
                "direction": trend,
                "delta": delta,
                "type": "sgv",
                "sgv": float(sg["sg"]),
                "date": epoch_ms,
                "dateString": date_str,
                "noise": 1,
            })
        return entries

    def _build_device_status(self, data: dict) -> list:
        """Build Nightscout devicestatus object."""
        model = (data.get("medicalDeviceInformation") or {}).get("modelNumber", "Medtronic Pump")
        battery_pct = data.get("pumpBatteryLevelPercent", data.get("conduitBatteryLevel", 100))
        reservoir = data.get("reservoirRemainingUnits", data.get("reservoirAmount", 0.0))
        iob_amount = (data.get("activeInsulin") or {}).get("amount", 0.0)
        status_msg = data.get("systemStatusMessage", "Normal")
        suspended = data.get("pumpSuspended", False)

        return [{
            "device": model,
            "pump": {
                "battery": {"status": "OK", "voltage": battery_pct},
                "reservoir": reservoir,
                "iob": {"bolusiob": iob_amount},
                "status": {"status": status_msg, "suspended": suspended},
            }
        }]

    def _build_treatments(self, markers: list, tz: ZoneInfo) -> list:
        """Transform Carelink markers into Nightscout treatments (bolus, carbs)."""
        treatments = []
        for m in markers:
            try:
                m_type = m.get("type", "")
                if m_type not in ["INSULIN", "MEAL"]:
                    continue

                ts_field = m.get("dateTime") or m.get("timestamp")
                if not ts_field:
                    continue
                    
                epoch_ms, date_str = self._parse_timestamp(ts_field, tz)
                
                treatment = {
                    "created_at": date_str,
                    "enteredBy": USER_AGENT,
                }

                if m_type == "INSULIN":
                    treatment["eventType"] = "Bolus"
                    treatment["insulin"] = float(m.get("amount", 0))
                elif m_type == "MEAL":
                    treatment["eventType"] = "Meal Bolus"
                    treatment["carbs"] = int(m.get("amount", 0))

                if treatment.get("insulin", 0) > 0 or treatment.get("carbs", 0) > 0:
                    treatments.append(treatment)
            except Exception as e:
                _LOGGER.debug(f"Skipping unparseable marker: {e}")
        return treatments

    async def upload_carelink_payload(self, data: dict, tz: ZoneInfo) -> dict:
        """Process and upload all Carelink data components to Nightscout."""
        await self.load_cache()
        self.purge_expired_cache()

        results = {}

        # 1. Device Status
        ds_items = self._build_device_status(data)
        ds_up, ds_skip = await self._post_batch("devicestatus", ds_items)
        results["devicestatus"] = (ds_up, ds_skip)

        # 2. SGV Entries
        sgs = data.get("sgs", [])
        if sgs:
            sgv_items = self._build_sgv_entries(sgs, tz)
            sgv_up, sgv_skip = await self._post_batch("entries", sgv_items)
            results["entries"] = (sgv_up, sgv_skip)

        # 3. Treatments (Bolus/Carbs)
        markers = data.get("markers", [])
        if markers:
            _LOGGER.info(f"Found {len(markers)} markers. Types present: {list(set([m.get('type') for m in markers]))}")
            trt_items = self._build_treatments(markers, tz)
            trt_up, trt_skip = await self._post_batch("treatments", trt_items)
            results["treatments"] = (trt_up, trt_skip)

        await self.save_cache()
        return results
