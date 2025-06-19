import logging
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
import hashlib
import json
import os
import aiohttp
import aiofiles

from homeassistant.components.sensor import SensorEntity
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.config_entries import ConfigEntry

from .const import (
    DOMAIN,
    CONF_USERNAME,
    CONF_PASSWORD,
    CONF_APP_ID,
    CONF_APP_SECRET,
    CONF_BASE_URL,
    CONF_START_MONTH,
)

_LOGGER = logging.getLogger(__name__)
SCAN_INTERVAL = timedelta(hours=1)
HISTORY_START_MONTH = "2024-01" 


def _sha256(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest().lower()


async def _async_get_token(session: aiohttp.ClientSession, username, password, app_id, app_secret, base_url):
    url = f"{base_url}/account/token?appId={app_id}"
    payload = {
        "appSecret": app_secret,
        "username": username,
        "password": _sha256(password),
    }
    async with session.post(url, json=payload, timeout=10) as resp:
        resp.raise_for_status()
        j = await resp.json()
        if not j.get("success"):
            raise Exception(f"Token request failed: {j.get('msg')}")
        return j["accessToken"]


async def _async_station_list(session, token, base_url):
    url = f"{base_url}/station/list"
    headers = {"Authorization": f"Bearer {token}"}
    async with session.post(url, headers=headers, json={}, timeout=10) as resp:
        resp.raise_for_status()
        return (await resp.json()).get("stationList", [])


async def _async_history(session, token, station_id, base_url):
    url = f"{base_url}/station/history"
    headers = {"Authorization": f"Bearer {token}"}
    items: list[dict] = []
    start = datetime.strptime(HISTORY_START_MONTH, "%Y-%m")
    end = datetime.now().replace(day=1)
    while start <= end:
        range_start = start
        range_end = min(range_start + relativedelta(months=11), end)
        payload = {
            "stationId": station_id,
            "granularity": 3,
            "startAt": range_start.strftime("%Y-%m"),
            "endAt": range_end.strftime("%Y-%m"),
        }
        async with session.post(url, headers=headers, json=payload, timeout=10) as resp:
            resp.raise_for_status()
            j = await resp.json()
            if not j.get("success"):
                raise Exception(f"History request failed: {j.get('msg')}")
            items.extend(j.get("stationDataItems", []))
        start = range_end + relativedelta(months=1)
    return items


async def _async_save_cache(hass: HomeAssistant, cache: dict):
    path = hass.config.path("deye_history.json")
    async with aiofiles.open(path, "w") as f:
        await f.write(json.dumps(cache, indent=2))


async def _async_clear_cache(hass: HomeAssistant):
    path = hass.config.path("deye_history.json")
    if os.path.exists(path):
        try:
            os.remove(path)
            _LOGGER.debug("Cleared old cache file: %s", path)
        except OSError as exc:
            _LOGGER.error("Failed to clear cache file: %s", exc)


class _BasicSensor(SensorEntity):
    _attr_has_entity_name = True

    def __init__(
        self,
        name: str,
        unique_id: str,
        native_value,
        unit: str | None = None,
        device_class: str | None = None,
        state_class: str | None = None,
        extra_state_attributes: dict | None = None,
    ) -> None:
        self._attr_name = name
        self._attr_unique_id = unique_id
        self._attr_native_value = native_value
        self._attr_native_unit_of_measurement = unit
        if device_class:
            self._attr_device_class = device_class
        if state_class:
            self._attr_state_class = state_class
        self._attr_extra_state_attributes = extra_state_attributes or {}

    async def async_update(self):
        return


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
):
    _LOGGER.debug("Setting up DeyeCloud sensors…")
    global HISTORY_START_MONTH
    HISTORY_START_MONTH = entry.data.get(CONF_START_MONTH, "2024-01")


    username = entry.data[CONF_USERNAME]
    password = entry.data[CONF_PASSWORD]
    app_id = entry.data[CONF_APP_ID]
    app_secret = entry.data[CONF_APP_SECRET]
    base_url = entry.data[CONF_BASE_URL]

    await _async_clear_cache(hass)

    entities: list[SensorEntity] = []
    cache: dict[str, list[dict]] = {}

    try:
        async with aiohttp.ClientSession() as session:
            token = await _async_get_token(session, username, password, app_id, app_secret, base_url)
            stations = await _async_station_list(session, token, base_url)

            now = datetime.now()
            this_year, this_month = now.year, now.month
            last_month_dt = now - relativedelta(months=1)
            prev_year, prev_month = last_month_dt.year, last_month_dt.month

            _METRICS = [
                ("Solar Generation", "generationValue"),
                ("Monthly Consumption", "consumptionValue"),
                ("Monthly Grid Export", "gridValue"),
                ("Monthly Grid Import", "purchaseValue"),
                ("Monthly Battery Charge", "chargeValue"),
                ("Monthly Battery Discharge", "dischargeValue"),
            ]

            for st in stations:
                station_id = st.get("id") or st.get("stationId")
                if not station_id:
                    _LOGGER.warning("Station without ID skipped: %s", st)
                    continue

                history = await _async_history(session, token, station_id, base_url)
                cache[str(station_id)] = history

                history_index = {(i["year"], i["month"]): i for i in history}

                for item in sorted(history, key=lambda x: (x["year"], x["month"])):
                    gen_val = item.get("generationValue")
                    if gen_val is None:
                        continue
                    y = item["year"]
                    m = f"{item['month']:02d}"
                    month_name = datetime(year=int(y), month=int(m), day=1).strftime("%b %Y")
                    name = f"Deye {station_id} {month_name}"
                    uid = f"{station_id}_raw_{y}_{m}"
                    entities.append(
                        _BasicSensor(
                            name=name,
                            unique_id=uid,
                            native_value=gen_val,
                            unit="kWh",
                            device_class="energy",
                            state_class="total_increasing",
                            extra_state_attributes=item,
                        )
                    )

                for (nice_name, key) in _METRICS:
                    cur_item = history_index.get((this_year, this_month), {})
                    cur_val = cur_item.get(key)
                    if cur_val is not None:
                        entities.append(
                            _BasicSensor(
                                name=f"{nice_name}",
                                unique_id=f"{station_id}_{key}_current_month",
                                native_value=cur_val,
                                unit="kWh",
                                device_class="energy",
                                state_class="total_increasing",
                                extra_state_attributes={
                                    "year": this_year,
                                    "month": this_month,
                                    "station_id": station_id,
                                },
                            )
                        )

                    prev_item = history_index.get((prev_year, prev_month), {})
                    prev_val = prev_item.get(key)
                    if prev_val is not None:
                        entities.append(
                            _BasicSensor(
                                name=f"{nice_name} (Tháng trước)",
                                unique_id=f"{station_id}_{key}_last_month",
                                native_value=prev_val,
                                unit="kWh",
                                device_class="energy",
                                state_class="total_increasing",
                                extra_state_attributes={
                                    "year": prev_year,
                                    "month": prev_month,
                                    "station_id": station_id,
                                },
                            )
                        )

        await _async_save_cache(hass, cache)
        async_add_entities(entities, update_before_add=True)

    except Exception as exc:
        _LOGGER.error("Failed to set up DeyeCloud sensors: %s", exc)
        raise
