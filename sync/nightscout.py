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
                str(entry.get("absolute", "")),
                str(entry.get("duration", "")),
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
            try:
                resp = await client.post(target_url, json=item)
                if resp.status_code in (200, 201):
                    uploaded += 1
                    if fp:
                        self.fingerprints[fp] = now_iso
                else:
                    _LOGGER.error(f"Nightscout {endpoint} upload error HTTP {resp.status_code}: {resp.text}")
            except httpx.RequestError as e:
                _LOGGER.warning(f"Nightscout {endpoint} network error: {e}")

        return uploaded, skipped

    @staticmethod
    def _parse_timestamp(iso_str: str, target_tz: ZoneInfo) -> tuple[int, str]:
        """Convert Carelink local ISO date string to epoch ms and ISO string with target timezone."""
        # Strip any trailing subseconds and timezone offsets (.000-00:00, .000Z, +00:00, etc.)
        # because Carelink timestamps are wall-clock local device times that Medtronic suffixes with fake UTC offsets.
        clean = re.sub(r"(\.\d+)?([+-]\d{2}:\d{2}|Z)?$", "", iso_str)
        dt = datetime.fromisoformat(clean)
        dt = dt.replace(tzinfo=target_tz)
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
        """Build Nightscout devicestatus object with pump, sensor, and conduit details."""
        model = (data.get("medicalDeviceInformation") or {}).get("modelNumber", "Medtronic Pump")
        battery_pct = data.get("pumpBatteryLevelPercent", data.get("conduitBatteryLevel", 100))
        reservoir = data.get("reservoirRemainingUnits", data.get("reservoirAmount", 0.0))
        iob_amount = (data.get("activeInsulin") or {}).get("amount", 0.0)
        status_msg = data.get("systemStatusMessage", "Normal")
        suspended = data.get("pumpSuspended", False)

        # Sensor details
        sensor_state = data.get("sensorState", "NO_ERROR_MESSAGE")
        sensor_dur_hours = data.get("sensorDurationHours")
        sensor_dur_mins = data.get("sensorDurationMinutes")
        if sensor_dur_mins is not None and sensor_dur_hours is None:
            sensor_dur_hours = round(sensor_dur_mins / 60, 1)
        calib_hours = data.get("timeToNextCalibHours")

        # Conduit / Uploader details
        conduit_battery = data.get("conduitBatteryLevel")
        conduit_in_range = data.get("conduitInRange")

        dev_status = {
            "device": model,
            "pump": {
                "battery": {"status": "OK", "voltage": battery_pct, "percent": battery_pct},
                "reservoir": reservoir,
                "iob": {"bolusiob": iob_amount},
                "status": {"status": status_msg, "suspended": suspended},
            },
            "sensor": {
                "sensorState": sensor_state,
            }
        }

        if sensor_dur_hours is not None:
            # Medtronic Carelink reports remaining sensor life in sensorDurationHours/Minutes (countdown to expiry)
            dev_status["sensor"]["sensorRemainingHours"] = sensor_dur_hours
            dev_status["sensor"]["sensorRemainingDays"] = round(sensor_dur_hours / 24, 1)
            dev_status["sensor"]["sensorAgeHours"] = max(0.0, round(168 - sensor_dur_hours, 1))
            dev_status["sensor"]["sensorAgeDays"] = max(0.0, round(7.0 - (sensor_dur_hours / 24), 1))

        if sensor_dur_mins is not None:
            dev_status["sensor"]["sensorRemainingMinutes"] = sensor_dur_mins

        if calib_hours is not None:
            dev_status["sensor"]["timeToNextCalibHours"] = calib_hours

        if conduit_battery is not None or conduit_in_range is not None:
            dev_status["uploader"] = {
                "battery": conduit_battery,
                "inRange": conduit_in_range,
            }

        return [dev_status]

    def _build_treatments(self, markers: list, data: dict, tz: ZoneInfo) -> list:
        """Transform Carelink markers into Nightscout treatments (bolus, carbs, basal, sensor changes, BG checks)."""
        treatments = []

        # 1. Automatic Sensor Change / Start Event
        # In Carelink, sensorDurationMinutes is the remaining countdown time on the 7-day (10080 min) lifespan
        dur_mins = data.get("sensorDurationMinutes")
        dur_hours = data.get("sensorDurationHours")
        if dur_mins is None and dur_hours is not None:
            dur_mins = int(dur_hours * 60)

        sgs = data.get("sgs", [])
        ref_ts = (sgs[-1]["timestamp"] if sgs else None) or data.get("medicalDeviceTime")

        if dur_mins is not None and dur_mins > 0 and ref_ts:
            try:
                epoch_ms, _ = self._parse_timestamp(ref_ts, tz)
                reading_dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
                # Calculate elapsed time from the 7-day lifespan (7 * 1440 = 10080 minutes)
                elapsed_mins = max(0, 10080 - dur_mins)
                sensor_start_utc = reading_dt - timedelta(minutes=elapsed_mins)
                sensor_start_local = sensor_start_utc.astimezone(tz)
                treatments.append({
                    "eventType": "Sensor Change",
                    "created_at": sensor_start_local.isoformat(),
                    "timestamp": int(sensor_start_utc.timestamp() * 1000),
                    "enteredBy": USER_AGENT,
                    "notes": f"Medtronic Sensor (Inserted {round(elapsed_mins / 1440, 1)}d ago, {round(dur_mins / 60, 1)}h remaining)",
                })
            except Exception as e:
                _LOGGER.debug(f"Could not calculate sensor start treatment: {e}")

        # 2. Carelink Event Markers
        for m in markers:
            try:
                m_type = m.get("type", "")
                if m_type not in ["INSULIN", "MEAL", "AUTO_BASAL_DELIVERY", "CALIBRATION", "BG_READING", "SENSOR_CHANGE", "SENSOR_START"]:
                    continue

                ts_field = m.get("dateTime") or m.get("timestamp")
                if not ts_field:
                    continue
                    
                epoch_ms, date_str = self._parse_timestamp(ts_field, tz)
                
                treatment = {
                    "timestamp": epoch_ms,
                    "created_at": date_str,
                    "enteredBy": USER_AGENT,
                }
                
                data_vals = m.get("data", {}).get("dataValues", {})

                if m_type == "INSULIN":
                    treatment["eventType"] = "Correction Bolus"
                    amount = data_vals.get("deliveredFastAmount", data_vals.get("programmedFastAmount", 0))
                    treatment["insulin"] = float(amount)
                elif m_type == "MEAL":
                    treatment["eventType"] = "Carb Correction"
                    treatment["carbs"] = int(float(data_vals.get("amount", 0)))
                elif m_type == "AUTO_BASAL_DELIVERY":
                    treatment["eventType"] = "Temp Basal"
                    treatment["duration"] = 5
                    amount = float(data_vals.get("bolusAmount", 0))
                    treatment["absolute"] = round(amount * 12, 3)
                elif m_type in ("CALIBRATION", "BG_READING"):
                    treatment["eventType"] = "BG Check"
                    treatment["glucose"] = float(data_vals.get("amount", data_vals.get("sg", 0)))
                    treatment["glucoseType"] = "Finger"
                elif m_type in ("SENSOR_CHANGE", "SENSOR_START"):
                    treatment["eventType"] = "Sensor Change"
                    treatment["notes"] = "Medtronic Sensor Changed"

                if (
                    treatment.get("insulin", 0) > 0
                    or treatment.get("carbs", 0) > 0
                    or treatment.get("absolute", 0) > 0
                    or treatment.get("glucose", 0) > 0
                    or treatment.get("eventType") == "Sensor Change"
                ):
                    treatments.append(treatment)
            except Exception as e:
                _LOGGER.debug(f"Skipping unparseable marker: {e}")
                
        # Merge meals and boluses that occur at the exact same timestamp
        merged = {}
        for t in treatments:
            key = f"{t['eventType']}_{t['created_at']}"
            if key not in merged:
                merged[key] = t
            else:
                if "carbs" in t:
                    merged[key]["carbs"] = t["carbs"]
                if "insulin" in t:
                    merged[key]["insulin"] = t["insulin"]
                if t.get("eventType") == "Carb Correction" and merged[key].get("eventType") == "Correction Bolus":
                    merged[key]["eventType"] = "Meal Bolus"
                
        return list(merged.values())

    async def upload_carelink_payload(self, data: dict, tz: ZoneInfo) -> dict:
        """Process and upload all Carelink data components to Nightscout."""
        await self.load_cache()
        self.purge_expired_cache()

        results = {}

        # 1. Device Status (Pump, Sensor, Conduit)
        ds_items = self._build_device_status(data)
        ds_up, ds_skip = await self._post_batch("devicestatus", ds_items)
        results["devicestatus"] = (ds_up, ds_skip)

        # 2. SGV Entries
        sgs = data.get("sgs", [])
        if sgs:
            sgv_items = self._build_sgv_entries(sgs, tz)
            sgv_up, sgv_skip = await self._post_batch("entries", sgv_items)
            results["entries"] = (sgv_up, sgv_skip)

        # 3. Treatments (Bolus/Carbs/Basal/Sensor Change/BG Checks)
        markers = data.get("markers", [])
        trt_items = self._build_treatments(markers, data, tz)
        if trt_items:
            trt_up, trt_skip = await self._post_batch("treatments", trt_items)
            results["treatments"] = (trt_up, trt_skip)

        await self.save_cache()
        return results
