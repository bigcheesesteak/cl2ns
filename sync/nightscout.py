"""cl2ns - Nightscout API Uploader

Clean, lightweight async uploader for Nightscout v1 REST API.
"""

import hashlib
import io
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import aiofiles
import httpx
from pypdf import PdfReader

_LOGGER = logging.getLogger("cl2ns.nightscout")

USER_AGENT = "cl2ns-sync-engine/1.0"
DEDUP_RETENTION_HOURS = 25


class NightscoutError(Exception):
    """Base exception for Nightscout API errors."""
    pass


def parse_device_settings_pdf(pdf_bytes: bytes, tz_name: str) -> dict | None:
    """Parse Medtronic Device Settings PDF into a Nightscout profile document."""
    if not pdf_bytes:
        return None

    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        if len(reader.pages) == 0:
            return None

        page1_text = reader.pages[0].extract_text() or ""
        page2_text = reader.pages[1].extract_text() if len(reader.pages) > 1 else ""

        # 1. DIA (Active Insulin Time)
        dia = "2.0"
        dia_match = re.search(
            r"Active\s+Insulin\s+Time(?:\s*\(h:mm\))?\s*(\d{1,2}):(\d{2})",
            page1_text,
            re.IGNORECASE,
        )
        if dia_match:
            hours = int(dia_match.group(1))
            minutes = int(dia_match.group(2))
            dia = str(round(hours + (minutes / 60.0), 2))

        # 2. SmartGuard Target
        target = "100"
        target_match = re.search(r"Target\s+(\d+(?:[.,]\d+)?)\s*mg/dL", page2_text, re.IGNORECASE)
        if not target_match:
            target_match = re.search(r"Target\s+(\d+(?:[.,]\d+)?)\s*mg/dL", page1_text, re.IGNORECASE)
        if target_match:
            target = target_match.group(1).replace(",", ".")

        # 3. Carbohydrate Ratio Schedule
        carb_section = ""
        carb_match = re.search(
            r"Time\s+Ratio(.*?)Time\s+Sensitivity",
            page1_text,
            re.DOTALL | re.IGNORECASE,
        )
        if carb_match:
            carb_section = carb_match.group(1)
        carb_raw = re.findall(r"(\d{1,2}:\d{2})\s+(\d+[.,]\d+)", carb_section)
        ratios = []
        seen_ratios = set()
        for t_str, val in carb_raw:
            t_formatted = t_str.zfill(5)
            if t_formatted not in seen_ratios:
                seen_ratios.add(t_formatted)
                ratios.append({"time": t_formatted, "value": val.replace(",", ".")})
        ratios.sort(key=lambda x: x["time"])
        if not ratios:
            ratios = [{"time": "00:00", "value": "10"}]

        # 4. Insulin Sensitivity (ISF) Schedule
        sens_section = ""
        sens_match = re.search(
            r"Time\s+Sensitivity(.*?)Time\s+Low(?:\s+High)?",
            page1_text,
            re.DOTALL | re.IGNORECASE,
        )
        if sens_match:
            sens_section = sens_match.group(1)
        sens_raw = re.findall(r"(\d{1,2}:\d{2})\s+(\d+(?:[.,]\d+)?)", sens_section)
        sens = []
        seen_sens = set()
        for t_str, val in sens_raw:
            t_formatted = t_str.zfill(5)
            if t_formatted not in seen_sens:
                seen_sens.add(t_formatted)
                sens.append({"time": t_formatted, "value": val.replace(",", ".")})
        sens.sort(key=lambda x: x["time"])
        if not sens:
            sens = [{"time": "00:00", "value": "50"}]

        # 5. Programmed Basal 1 Schedule
        basal_section = ""
        basal_match = re.search(
            r"Basal\s*1\s*\(active\)(.*?)(?:--\s*--|$)",
            page1_text,
            re.DOTALL | re.IGNORECASE,
        )
        if basal_match:
            basal_section = basal_match.group(1)
        basal_raw = re.findall(
            r"(\d{1,2}:\d{2})(?:\s*-\s*\d{1,2}:\d{2})?\s+(\d+(?:[.,]\d+)?)",
            basal_section,
        )
        basal = []
        seen_basal = set()
        for t_str, val in basal_raw:
            t_formatted = t_str.zfill(5)
            if t_formatted not in seen_basal:
                seen_basal.add(t_formatted)
                basal.append({"time": t_formatted, "value": val.replace(",", ".")})
        basal.sort(key=lambda x: x["time"])
        if not basal:
            basal = [{"time": "00:00", "value": "0.0"}]

        now_iso = datetime.now(timezone.utc).isoformat()
        return {
            "defaultProfile": "SmartGuard",
            "store": {
                "SmartGuard": {
                    "dia": dia,
                    "carbratio": ratios,
                    "sens": sens,
                    "basal": basal,
                    "target_low": [{"time": "00:00", "value": target}],
                    "target_high": [{"time": "00:00", "value": target}],
                    "units": "mg/dl",
                    "timezone": tz_name,
                }
            },
            "startDate": now_iso,
            "enteredBy": USER_AGENT,
        }
    except Exception as e:
        _LOGGER.error(f"Failed parsing device settings PDF: {e}")
        return None


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
                str(entry.get("glucose", "")),
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
    def _parse_timestamp(iso_str: str | int | float, target_tz: ZoneInfo) -> tuple[int, str]:
        """Convert Carelink local ISO date string or epoch ms to epoch ms and ISO string with target timezone."""
        if isinstance(iso_str, (int, float)):
            dt = datetime.fromtimestamp(iso_str / 1000, tz=timezone.utc).astimezone(target_tz)
            return int(iso_str), dt.isoformat()
        if isinstance(iso_str, str) and iso_str.isdigit():
            epoch_val = int(iso_str)
            dt = datetime.fromtimestamp(epoch_val / 1000, tz=timezone.utc).astimezone(target_tz)
            return epoch_val, dt.isoformat()

        # Strip any trailing subseconds and timezone offsets (.000-00:00, .000Z, +00:00, etc.)
        # because Carelink timestamps are wall-clock local device times that Medtronic suffixes with fake UTC offsets.
        clean = re.sub(r"(\.\d+)?([+-]\d{2}:\d{2}|Z)?$", "", str(iso_str))
        dt = datetime.fromisoformat(clean)
        dt = dt.replace(tzinfo=target_tz)
        epoch_ms = int(dt.timestamp() * 1000)
        return epoch_ms, dt.isoformat()

    def _build_sgv_entries(self, sgs: list, tz: ZoneInfo) -> list:
        """Transform Carelink SGS array into Nightscout SGV entries."""
        valid_sgs = [
            s for s in sgs
            if s.get("sensorState") == "NO_ERROR_MESSAGE"
            and (s.get("timestamp") or s.get("datetime") or s.get("dateTime"))
        ]
        entries = []

        for idx, sg in enumerate(valid_sgs):
            ts_str = sg.get("timestamp") or sg.get("datetime") or sg.get("dateTime")
            epoch_ms, date_str = self._parse_timestamp(ts_str, tz)
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

    def _build_device_status(self, data: dict, tz: ZoneInfo = None) -> list:
        """Build Nightscout devicestatus object with pump, sensor, and conduit details."""
        model = (data.get("medicalDeviceInformation") or {}).get("modelNumber", "Medtronic Pump")
        battery_pct = data.get("pumpBatteryLevelPercent", data.get("conduitBatteryLevel", 100))
        reservoir = data.get("reservoirRemainingUnits", data.get("reservoirAmount", 0.0))
        iob_amount = (data.get("activeInsulin") or {}).get("amount", 0.0)
        suspended = data.get("pumpSuspended", False)
        shield_state = (data.get("therapyAlgorithmState") or {}).get("autoModeShieldState") or data.get("systemStatusMessage", "Normal")

        # Resolve IOB / devicestatus timestamp
        active_insulin = data.get("activeInsulin") or {}
        active_insulin_ts = (
            active_insulin.get("datetime")
            or active_insulin.get("timestamp")
            or data.get("medicalDeviceTime")
        )
        if active_insulin_ts and tz:
            try:
                _, iob_dt = self._parse_timestamp(active_insulin_ts, tz)
            except Exception:
                iob_dt = datetime.now(timezone.utc).isoformat()
        else:
            iob_dt = datetime.now(timezone.utc).isoformat()

        # Limits / SmartGuard Targets
        limits = data.get("limits") or []
        last_limit = limits[-1] if isinstance(limits, list) and len(limits) > 0 else {}
        target_low = last_limit.get("lowLimit") or last_limit.get("targetLow") or last_limit.get("target") or 100
        target_high = last_limit.get("highLimit") or last_limit.get("targetHigh") or last_limit.get("target") or 100

        # Extended pump telemetry
        extended = {
            "reservoirPercent": data.get("reservoirLevelPercent"),
            "autoModeShieldState": (data.get("therapyAlgorithmState") or {}).get("autoModeShieldState"),
            "autoModeReadiness": (data.get("therapyAlgorithmState") or {}).get("autoModeReadinessState"),
            "maxAutoBasalRate": data.get("maxAutoBasalRate"),
            "timeInRange": data.get("timeInRange"),
            "averageSG": data.get("averageSG"),
            "belowHypoLimit": data.get("belowHypoLimit"),
            "aboveHyperLimit": data.get("aboveHyperLimit"),
            "targetLow": target_low,
            "targetHigh": target_high,
        }

        dev_status = {
            "device": model,
            "created_at": iob_dt,
            "pump": {
                "battery": {"status": "OK", "voltage": battery_pct, "percent": battery_pct},
                "reservoir": reservoir,
                "iob": {"iob": iob_amount, "bolusiob": iob_amount},
                "status": {"status": shield_state, "suspended": suspended},
                "extended": extended,
            },
            "openaps": {
                "iob": {
                    "iob": iob_amount,
                    "bolusiob": iob_amount,
                    "timestamp": iob_dt,
                },
                "suggested": {
                    "target": target_low,
                    "minPredBG": target_low,
                    "maxPredBG": target_high,
                },
            },
            "sensor": {
                "sensorState": data.get("sensorState", "NO_ERROR_MESSAGE"),
                "calFreeSensor": data.get("calFreeSensor", True),
            },
        }

        # Sensor details
        sensor_model = (data.get("cgmInfo") or {}).get("sensorProductModel")
        if sensor_model is not None:
            dev_status["sensor"]["sensorModel"] = sensor_model

        sensor_dur_hours = data.get("sensorDurationHours")
        sensor_dur_mins = data.get("sensorDurationMinutes")
        if sensor_dur_mins is not None and sensor_dur_hours is None:
            sensor_dur_hours = round(sensor_dur_mins / 60, 1)

        if sensor_dur_hours is not None:
            dev_status["sensor"]["sensorRemainingHours"] = sensor_dur_hours
            dev_status["sensor"]["sensorRemainingDays"] = round(sensor_dur_hours / 24, 1)
            dev_status["sensor"]["sensorAgeHours"] = max(0.0, round(168 - sensor_dur_hours, 1))
            dev_status["sensor"]["sensorAgeDays"] = max(0.0, round(7.0 - (sensor_dur_hours / 24), 1))

        if sensor_dur_mins is not None:
            dev_status["sensor"]["sensorRemainingMinutes"] = sensor_dur_mins

        calib_hours = data.get("timeToNextCalibHours")
        if calib_hours is not None:
            dev_status["sensor"]["timeToNextCalibHours"] = calib_hours

        # Conduit / Uploader details
        conduit_battery = data.get("conduitBatteryLevel")
        conduit_status = data.get("conduitBatteryStatus")
        conduit_in_range = data.get("conduitInRange")

        uploader = {}
        if conduit_battery is not None:
            uploader["battery"] = conduit_battery
            uploader["conduitBatteryLevel"] = conduit_battery
        if conduit_status is not None:
            uploader["conduitBatteryStatus"] = conduit_status
        if conduit_in_range is not None:
            uploader["inRange"] = conduit_in_range
        if uploader:
            dev_status["uploader"] = uploader

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
        ref_ts = None
        for s in reversed(sgs):
            ts = s.get("timestamp") or s.get("datetime") or s.get("dateTime")
            if ts:
                ref_ts = ts
                break

        if not ref_ts:
            last_sg = data.get("lastSG") or {}
            ref_ts = (
                last_sg.get("timestamp")
                or last_sg.get("datetime")
                or last_sg.get("dateTime")
                or data.get("medicalDeviceTime")
            )

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
                if m_type not in [
                    "INSULIN",
                    "MEAL",
                    "AUTO_BASAL_DELIVERY",
                    "CALIBRATION",
                    "BG_READING",
                    "SENSOR_CHANGE",
                    "SENSOR_START",
                ]:
                    continue

                ts_field = m.get("dateTime") or m.get("timestamp") or m.get("datetime")
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
                    activation_type = data_vals.get("activationType")
                    if activation_type == "AUTOCORRECTION":
                        treatment["eventType"] = "Correction Bolus"
                        treatment["notes"] = "SmartGuard Auto-Correction"
                    else:
                        treatment["eventType"] = "Bolus Wizard"
                    amount = data_vals.get("deliveredFastAmount", data_vals.get("programmedFastAmount", 0))
                    treatment["insulin"] = float(amount or 0)
                elif m_type == "MEAL":
                    treatment["eventType"] = "Carb Correction"
                    treatment["carbs"] = int(float(data_vals.get("amount", 0)))
                elif m_type == "AUTO_BASAL_DELIVERY":
                    treatment["eventType"] = "Temp Basal"
                    treatment["duration"] = 5
                    amount = float(data_vals.get("bolusAmount", 0))
                    treatment["absolute"] = round(amount * 12, 3)
                elif m_type == "CALIBRATION":
                    treatment["eventType"] = "Sensor Calibration"
                    val = data_vals.get("unitValue") or data_vals.get("amount") or data_vals.get("sg", 0)
                    treatment["glucose"] = float(val or 0)
                    treatment["glucoseType"] = "Finger"
                    success = data_vals.get("calibrationSuccess", True)
                    cal_type = data_vals.get("calibrationType", "CALIBRATION")
                    treatment["notes"] = f"Calibration {'Accepted' if success else 'Rejected'} ({cal_type})"
                elif m_type == "BG_READING":
                    treatment["eventType"] = "BG Check"
                    val = data_vals.get("unitValue") or data_vals.get("amount") or data_vals.get("sg", 0)
                    treatment["glucose"] = float(val or 0)
                    treatment["glucoseType"] = "Finger"
                elif m_type in ("SENSOR_CHANGE", "SENSOR_START"):
                    treatment["eventType"] = "Sensor Change"
                    treatment["notes"] = "Medtronic Sensor Changed"

                if (
                    treatment.get("insulin", 0) > 0
                    or treatment.get("carbs", 0) > 0
                    or treatment.get("absolute", 0) > 0
                    or treatment.get("glucose", 0) > 0
                    or treatment.get("eventType") in ("Sensor Change", "Sensor Calibration", "Announcement")
                ):
                    treatments.append(treatment)
            except Exception as e:
                _LOGGER.debug(f"Skipping unparseable marker: {e}")

        # 3. Forward Pump Alerts as Announcements / Notes
        alarms_to_process = []
        last_alarm = data.get("lastAlarm")
        if isinstance(last_alarm, dict) and last_alarm:
            alarms_to_process.append(last_alarm)

        cleared_notifs = (data.get("notificationHistory") or {}).get("clearedNotifications", [])
        if isinstance(cleared_notifs, list):
            for notif in cleared_notifs:
                if isinstance(notif, dict) and notif:
                    alarms_to_process.append(notif)

        for alarm in alarms_to_process:
            ts_field = alarm.get("dateTime") or alarm.get("timestamp") or alarm.get("datetime")
            fault_id = alarm.get("faultId") or alarm.get("faultID") or alarm.get("id")
            alert_type = alarm.get("alertType") or alarm.get("alarmType") or alarm.get("type") or alarm.get("message")
            if ts_field and (fault_id is not None or alert_type):
                try:
                    epoch_ms, date_str = self._parse_timestamp(ts_field, tz)
                    if fault_id is not None and alert_type:
                        alarm_notes = f"Medtronic Alert {fault_id}: {alert_type}"
                    elif fault_id is not None:
                        alarm_notes = f"Medtronic Alert {fault_id}"
                    else:
                        alarm_notes = f"Medtronic Alert: {alert_type}"

                    treatments.append({
                        "eventType": "Announcement",
                        "timestamp": epoch_ms,
                        "created_at": date_str,
                        "enteredBy": USER_AGENT,
                        "notes": alarm_notes,
                    })
                except Exception as e:
                    _LOGGER.debug(f"Skipping unparseable alarm: {e}")

        # 4. Merge meals and boluses that occur at the exact same timestamp
        merged = {}
        for t in treatments:
            bucket = (
                t["eventType"]
                if t["eventType"] in ("Temp Basal", "BG Check", "Sensor Calibration", "Sensor Change", "Announcement")
                else "MEAL_BOLUS"
            )
            key = f"{bucket}_{t['created_at']}"
            if key not in merged:
                merged[key] = t
            else:
                if "carbs" in t:
                    merged[key]["carbs"] = t["carbs"]
                if "insulin" in t:
                    merged[key]["insulin"] = t["insulin"]
                if "notes" in t and t["notes"]:
                    existing_notes = merged[key].get("notes")
                    if existing_notes:
                        if t["notes"] not in existing_notes:
                            merged[key]["notes"] = f"{existing_notes}; {t['notes']}"
                    else:
                        merged[key]["notes"] = t["notes"]

            if merged[key].get("carbs", 0) > 0 and merged[key].get("insulin", 0) > 0:
                merged[key]["eventType"] = "Meal Bolus"

        return list(merged.values())

    @staticmethod
    def parse_device_settings_pdf(pdf_bytes: bytes, tz_name: str) -> dict | None:
        """Parse Medtronic Device Settings PDF into Nightscout profile document."""
        return parse_device_settings_pdf(pdf_bytes, tz_name)

    async def upload_profile(self, profile_doc: dict) -> bool:
        """Post profile document to Nightscout /api/v1/profile."""
        if not profile_doc:
            return False
        client = await self._get_client()
        target_url = f"{self.url}/api/v1/profile"
        try:
            resp = await client.post(target_url, json=profile_doc)
            if resp.status_code in (200, 201):
                _LOGGER.info("Nightscout profile synchronized successfully.")
                return True
            else:
                _LOGGER.error(f"Nightscout profile upload error HTTP {resp.status_code}: {resp.text}")
                return False
        except httpx.RequestError as e:
            _LOGGER.warning(f"Nightscout profile upload network error: {e}")
            return False

    async def upload_carelink_payload(self, data: dict, tz: ZoneInfo) -> dict:
        """Process and upload all Carelink data components to Nightscout."""
        await self.load_cache()
        self.purge_expired_cache()

        results = {}

        # 1. Device Status (Pump, Sensor, Conduit)
        ds_items = self._build_device_status(data, tz)
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
