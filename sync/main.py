"""cl2ns - Carelink Cloud to Nightscout Standalone Sync Engine

Main entry point and adaptive polling daemon.
"""

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from carelink import CarelinkClient, CarelinkError, CarelinkAuthError
from nightscout import NightscoutUploader, NightscoutError

MS_TIMEZONE_MAP = {
    "W. Europe Standard Time": "Europe/Berlin",
    "Central European Summer Time": "Europe/Amsterdam",
    "Romance Standard Time": "Europe/Paris",
    "GMT Standard Time": "Europe/London",
    "Eastern Standard Time": "America/New_York",
    "Central Standard Time": "America/Chicago",
    "Pacific Standard Time": "America/Los_Angeles",
}


def setup_logger():
    """Configure concise, readable stdout logger for container output."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, log_level, logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(handler)

    return root


async def main():
    logger = setup_logger()
    sys.stdout.flush()

    logger.info("==================================================")
    logger.info("             cl2ns :: Carelink -> Nightscout       ")
    logger.info("==================================================")

    # Load configuration from environment
    carelink_token = os.getenv("CARELINK_TOKEN") or os.getenv("CARELINK_ACCESS_TOKEN")
    carelink_refresh_token = os.getenv("CARELINK_REFRESH_TOKEN")
    client_id = os.getenv("CARELINK_CLIENT_ID")
    client_secret = os.getenv("CARELINK_CLIENT_SECRET")
    mag_identifier = os.getenv("CARELINK_MAG_IDENTIFIER")
    patient_id = os.getenv("CARELINK_PATIENT_ID")

    nightscout_url = os.getenv("NIGHTSCOUT_URL")
    nightscout_api_secret = os.getenv("NIGHTSCOUT_API_SECRET")

    sync_interval_sec = int(os.getenv("SYNC_INTERVAL", "300"))
    if sync_interval_sec < 30:
        sync_interval_sec = 30

    data_dir = os.getenv("DATA_DIR", "/data")
    os.makedirs(data_dir, exist_ok=True)

    # Validate configuration
    token_file_exists = os.path.exists(os.path.join(data_dir, "carelink_tokens.json"))
    missing = []

    if not nightscout_url:
        missing.append("NIGHTSCOUT_URL")
    if not nightscout_api_secret:
        missing.append("NIGHTSCOUT_API_SECRET")
    if not token_file_exists and (not carelink_token or not carelink_refresh_token or not client_id):
        if not carelink_token:
            missing.append("CARELINK_TOKEN")
        if not carelink_refresh_token:
            missing.append("CARELINK_REFRESH_TOKEN")
        if not client_id:
            missing.append("CARELINK_CLIENT_ID")

    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    logger.info(f"Target Nightscout: {nightscout_url}")
    logger.info(f"Base Sync Cadence: {sync_interval_sec}s")
    logger.info(f"Data directory:   {data_dir}")

    # Initialize Nightscout uploader
    ns_uploader = NightscoutUploader(
        url=nightscout_url,
        api_secret=nightscout_api_secret,
        data_dir=data_dir,
    )

    if await ns_uploader.test_connection():
        logger.info("Nightscout connection test: SUCCESS")
    else:
        logger.warning("Nightscout connection test: FAILED (will retry during sync loop)")

    # Initialize Carelink Client
    cl_client = CarelinkClient(
        access_token=carelink_token,
        refresh_token=carelink_refresh_token,
        client_id=client_id,
        client_secret=client_secret,
        mag_identifier=mag_identifier,
        patient_id=patient_id,
        data_dir=data_dir,
    )

    # Shutdown signal handler
    shutdown_event = asyncio.Event()

    def signal_handler():
        logger.info("Shutdown signal received. Exiting loop gracefully...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass

    last_processed_sg_timestamp = None
    retry_count = 0
    max_fast_retries = 3
    fast_retry_interval_sec = 60

    logger.info("Starting synchronization daemon...")

    while not shutdown_event.is_set():
        now_utc = datetime.now(timezone.utc)
        logger.info(f"--- Sync Cycle [{now_utc.strftime('%H:%M:%S UTC')}] ---")

        sleep_time = sync_interval_sec

        try:
            patient_data = await cl_client.fetch_recent_data()
            if not patient_data:
                logger.warning("No data payload returned from Carelink.")
            else:
                logger.debug(f"Response top-level keys: {list(patient_data.keys())}")
                logger.debug(
                    f"Raw values: lastSG={patient_data.get('lastSG')} | "
                    f"pumpBattery={patient_data.get('pumpBatteryLevelPercent')} | "
                    f"reservoirRemaining={patient_data.get('reservoirRemainingUnits')} | "
                    f"reservoirAmount={patient_data.get('reservoirAmount')} | "
                    f"activeInsulin={patient_data.get('activeInsulin')} | "
                    f"sgs count={len(patient_data.get('sgs', []))}"
                )
                sgs_raw = patient_data.get("sgs", [])
                if sgs_raw:
                    logger.debug(f"First SG entry: {sgs_raw[0]}")
                    logger.debug(f"Last SG entry:  {sgs_raw[-1]}")
                logger.info(
                    f"Device Status: sensorState={patient_data.get('sensorState')} | "
                    f"systemStatus={patient_data.get('systemStatusMessage')} | "
                    f"conduitInRange={patient_data.get('conduitInRange')} | "
                    f"pumpComm={patient_data.get('pumpCommunicationState')} | "
                    f"sensorComm={patient_data.get('gstCommunicationState')} | "
                    f"medicalDeviceTime={patient_data.get('medicalDeviceTime')}"
                )

                sgs = patient_data.get("sgs", [])
                # Filter out warmup/error readings (sg=0) and pick the latest (last) entry
                valid_sgs = [s for s in sgs if s.get("sg", 0) > 0]
                latest_sg_obj = valid_sgs[-1] if valid_sgs else None
                latest_sg_val = latest_sg_obj.get("sg") if latest_sg_obj else "N/A"
                latest_sg_ts = (latest_sg_obj.get("datetime") or latest_sg_obj.get("timestamp")) if latest_sg_obj else None
                battery = (patient_data.get("medicalDeviceInformation") or {}).get("pumpBatteryLevelPercent",
                          patient_data.get("pumpBatteryLevelPercent", "N/A"))
                reservoir = patient_data.get("reservoirRemainingUnits",
                           (patient_data.get("activeInsulin") or {}).get("amount", "N/A"))

                # Resolve Timezone
                env_tz = os.getenv("TIMEZONE")
                if env_tz:
                    try:
                        tz = ZoneInfo(env_tz)
                    except Exception:
                        logger.warning(f"Invalid TIMEZONE '{env_tz}', falling back to UTC.")
                        tz = ZoneInfo("UTC")
                else:
                    cl_tz = patient_data.get("clientTimeZoneName", "UTC")
                    tz_name = MS_TIMEZONE_MAP.get(cl_tz, cl_tz)
                    try:
                        tz = ZoneInfo(tz_name)
                    except Exception:
                        tz = ZoneInfo("UTC")

                # Upload to Nightscout
                upload_stats = await ns_uploader.upload_carelink_payload(patient_data, tz)
                entries_stat = upload_stats.get("entries", (0, 0))
                treatments_stat = upload_stats.get("treatments", (0, 0))

                logger.info(
                    f"Carelink Data: Glucose={latest_sg_val} mg/dL | "
                    f"Battery={battery}% | Reservoir={reservoir} U"
                )
                logger.info(
                    f"Nightscout Upload: SGV entries={entries_stat[0]} (skipped={entries_stat[1]}) | "
                    f"Treatments={treatments_stat[0]} (skipped={treatments_stat[1]})"
                )

                # Smart Polling Check: Has a new SGV reading arrived?
                if latest_sg_ts and latest_sg_ts == last_processed_sg_timestamp:
                    if retry_count < max_fast_retries:
                        retry_count += 1
                        sleep_time = fast_retry_interval_sec
                        logger.info(
                            f"No new SGV reading yet (last timestamp: {latest_sg_ts}). "
                            f"Scheduling quick retry #{retry_count} in {fast_retry_interval_sec}s..."
                        )
                    else:
                        retry_count = 0
                        logger.info(f"Max retries reached. Resuming normal {sync_interval_sec}s cadence.")
                else:
                    if last_processed_sg_timestamp is not None:
                        logger.info("New SGV reading received and synced!")
                    last_processed_sg_timestamp = latest_sg_ts
                    retry_count = 0

        except CarelinkAuthError as e:
            logger.error(f"Carelink Auth Error: {e}")
        except CarelinkError as e:
            logger.error(f"Carelink API Error: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error during sync cycle: {e}")

        sys.stdout.flush()

        logger.info(f"Next sync check in {sleep_time}s.")
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_time)
        except asyncio.TimeoutError:
            pass

    logger.info("Closing sessions...")
    await cl_client.close()
    await ns_uploader.close()
    logger.info("cl2ns daemon stopped successfully.")


if __name__ == "__main__":
    asyncio.run(main())
