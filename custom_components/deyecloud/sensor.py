import logging
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
import hashlib
import json
import os
import aiohttp
import aiofiles
import asyncio

from homeassistant.components.sensor import SensorEntity, SensorEntityDescription
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
SCAN_INTERVAL = timedelta(minutes=1)  # Đổi từ 5 phút thành 1 phút
HISTORY_START_MONTH = "2024-01"

def _sha256(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest().lower()

async def _async_get_token(session: aiohttp.ClientSession, username, password, app_id, app_secret, base_url):
    url = f"{base_url}/account/token?appId={app_id}"
    _LOGGER.debug("Đang yêu cầu token từ API: %s", url)
    payload = {
        "appSecret": app_secret,
        "username": username,
        "password": _sha256(password),
    }
    async with session.post(url, json=payload, timeout=10) as resp:
        resp.raise_for_status()
        j = await resp.json()
        if not j.get("success"):
            _LOGGER.error("Yêu cầu token thất bại: %s", j.get("msg"))
            raise Exception(f"Token request failed: {j.get('msg')}")
        _LOGGER.debug("Yêu cầu token thành công")
        return j["accessToken"]

async def _async_station_list(session, token, base_url):
    url = f"{base_url}/station/list"
    _LOGGER.debug("Đang lấy danh sách trạm từ API: %s", url)
    headers = {"Authorization": f"Bearer {token}"}
    async with session.post(url, headers=headers, json={}, timeout=10) as resp:
        resp.raise_for_status()
        stations = (await resp.json()).get("stationList", [])
        _LOGGER.info("Nhận được %d trạm từ API", len(stations))
        return stations

async def _async_history(session, token, station_id, base_url):
    url = f"{base_url}/station/history"
    headers = {"Authorization": f"Bearer {token}"}
    items: list[dict] = []
    start = datetime.strptime(HISTORY_START_MONTH, "%Y-%m")
    end = datetime.now().replace(day=1)
    _LOGGER.debug("Tải lịch sử tháng cho station_id %s từ %s đến %s", station_id, start.strftime("%Y-%m"), end.strftime("%Y-%m"))
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
                _LOGGER.error("Yêu cầu lịch sử tháng thất bại cho station_id %s: %s", station_id, j.get("msg"))
                raise Exception(f"History request failed: {j.get('msg')}")
            items.extend(j.get("stationDataItems", []))
        start = range_end + relativedelta(months=1)
    _LOGGER.debug("Nhận được %d bản ghi tháng cho station_id %s", len(items), station_id)
    return items

async def _async_daily_history(session, token, station_id, base_url, start_date, end_date):
    url = f"{base_url}/station/history"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {
        "stationId": station_id,
        "granularity": 2,
        "startAt": start_date,
        "endAt": end_date,
    }
    _LOGGER.debug("Tải dữ liệu ngày cho station_id %s từ %s đến %s", station_id, start_date, end_date)
    async with session.post(url, headers=headers, json=payload, timeout=10) as resp:
        resp.raise_for_status()
        j = await resp.json()
        if not j.get("success"):
            _LOGGER.error("Yêu cầu lịch sử ngày thất bại cho station_id %s: %s", station_id, j.get("msg"))
            raise Exception(f"Daily history request failed: {j.get('msg')}")
        items = j.get("stationDataItems", [])
        _LOGGER.debug("Nhận được %d bản ghi ngày cho station_id %s", len(items), station_id)
        return items

async def _async_get_device_list(session, token, base_url, stations):
    url = f"{base_url}/station/device"
    _LOGGER.debug("Đang lấy danh sách thiết bị từ API: %s", url)
    headers = {"Authorization": f"Bearer {token}"}
    station_ids = [st.get("id") or st.get("stationId") for st in stations if st.get("id") or st.get("stationId")]
    if not station_ids:
        _LOGGER.warning("Không có stationId nào để gửi yêu cầu")
        return []
    payload = {
        "page": 1,
        "size": 20,
        "stationIds": station_ids
    }
    _LOGGER.debug("Gửi payload: %s", payload)
    async with session.post(url, headers=headers, json=payload, timeout=10) as resp:
        resp.raise_for_status()
        j = await resp.json()
        if not j.get("success"):
            _LOGGER.error("Yêu cầu danh sách thiết bị thất bại: %s", j.get("msg"))
            raise Exception(f"Device list request failed: {j.get('msg')}")
        _LOGGER.debug("Nhận được danh sách thiết bị: %s", j)
        return [item["deviceSn"] for item in j.get("deviceListItems", []) if item.get("deviceType") == "INVERTER"]

async def _async_get_device_status(session, token, base_url, device_list):
    url = f"{base_url}/device/latest"
    _LOGGER.debug("Đang lấy trạng thái thiết bị từ API: %s với device_list: %s", url, device_list)
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"deviceList": device_list}
    async with session.post(url, headers=headers, json=payload, timeout=10) as resp:
        resp.raise_for_status()
        j = await resp.json()
        if not j.get("success"):
            _LOGGER.error("Yêu cầu trạng thái thiết bị thất bại: %s", j.get("msg"))
            raise Exception(f"Device status request failed: {j.get('msg')}")
        _LOGGER.debug("Nhận được trạng thái thiết bị: %s", j)
        return j.get("deviceDataList", [])

async def _async_save_cache(hass: HomeAssistant, cache: dict, filename: str = "deye_history.json"):
    path = hass.config.path(filename)
    _LOGGER.debug("Lưu cache vào tệp: %s", path)
    config_dir = os.path.dirname(path)
    if not os.access(config_dir, os.W_OK):
        _LOGGER.error("Không có quyền ghi vào thư mục: %s", config_dir)
        raise PermissionError(f"Không có quyền ghi vào thư mục: {config_dir}")
    try:
        async with aiofiles.open(path, "w") as f:
            await f.write(json.dumps(cache, indent=2))
        _LOGGER.info("Lưu thành công cache vào %s", path)
    except OSError as exc:
        _LOGGER.error("Lỗi khi lưu cache vào %s: %s", path, exc)
        raise

async def _async_clear_cache(hass: HomeAssistant, filename: str = "deye_history.json"):
    path = hass.config.path(filename)
    _LOGGER.debug("Kiểm tra xóa tệp cache: %s", path)
    if os.path.exists(path):
        try:
            os.remove(path)
            _LOGGER.info("Xóa tệp cache thành công: %s", path)
        except OSError as exc:
            _LOGGER.error("Lỗi khi xóa tệp cache: %s", exc)
            raise
    else:
        _LOGGER.debug("Tệp cache không tồn tại: %s", path)

async def _async_save_daily_cache(hass: HomeAssistant, cache: dict):
    await _async_save_cache(hass, cache, "deye_daily_history.json")

async def _async_clear_daily_cache(hass: HomeAssistant):
    await _async_clear_cache(hass, "deye_daily_history.json")

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
        hass: HomeAssistant = None,
        entry: ConfigEntry = None,
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
        self._hass = hass
        self._entry = entry
        self._token = None

    async def async_update(self):
        if not self._hass or not self._entry:
            _LOGGER.warning("Hass or entry not available for update")
            return

        username = self._entry.data.get(CONF_USERNAME)
        password = self._entry.data.get(CONF_PASSWORD)
        app_id = self._entry.data.get(CONF_APP_ID)
        app_secret = self._entry.data.get(CONF_APP_SECRET)
        base_url = self._entry.data.get(CONF_BASE_URL)

        try:
            async with aiohttp.ClientSession() as session:
                # Get token if not available
                if not self._token:
                    self._token = await _async_get_token(session, username, password, app_id, app_secret, base_url)

                # Determine station_id or device_sn from unique_id
                if "device_" in self._attr_unique_id:
                    device_sn = self._attr_unique_id.split("device_")[1].split("_")[0]
                    device_list = [device_sn]
                    device_data_list = await _async_get_device_status(session, self._token, base_url, device_list)
                    if device_data_list:
                        device_data = device_data_list[0]
                        for data in device_data.get("dataList", []):
                            if f"device_{device_sn}_{data['key']}" == self._attr_unique_id:
                                self._attr_native_value = data["value"]
                                self._attr_native_unit_of_measurement = data["unit"]
                                self._attr_extra_state_attributes = {
                                    "device_sn": device_sn,
                                    "device_type": device_data.get("deviceType"),
                                    "device_state": device_data.get("deviceState"),
                                    "collection_time": device_data.get("collectionTime"),
                                }
                                break
                else:
                    station_id = self._attr_unique_id.split("_")[0]
                    now = datetime.now()
                    this_year, this_month = now.year, now.month
                    last_month_dt = now - relativedelta(months=1)
                    prev_year, prev_month = last_month_dt.year, last_month_dt.month
                    today = now.strftime("%Y-%m-%d")
                    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
                    day_before_yesterday = (now - timedelta(days=2)).strftime("%Y-%m-%d")

                    if "raw" in self._attr_unique_id:
                        # Update monthly raw data
                        history = await _async_history(session, self._token, station_id, base_url)
                        history_index = {(i["year"], i["month"]): i for i in history}
                        y = int(self._attr_unique_id.split("_raw_")[1].split("_")[0])
                        m = int(self._attr_unique_id.split("_raw_")[1].split("_")[1])
                        item = history_index.get((y, m))
                        if item and item.get("generationValue") is not None:
                            self._attr_native_value = item["generationValue"]
                            self._attr_extra_state_attributes = item
                    elif "_current_month" in self._attr_unique_id or "_last_month" in self._attr_unique_id:
                        # Update monthly metrics
                        history = await _async_history(session, self._token, station_id, base_url)
                        history_index = {(i["year"], i["month"]): i for i in history}
                        key = self._attr_unique_id.split("_")[-2]
                        if "_current_month" in self._attr_unique_id:
                            item = history_index.get((this_year, this_month))
                            self._attr_extra_state_attributes = {"year": this_year, "month": this_month, "station_id": station_id}
                        else:  # last_month
                            item = history_index.get((prev_year, prev_month))
                            self._attr_extra_state_attributes = {"year": prev_year, "month": prev_month, "station_id": station_id}
                        if item and item.get(key) is not None:
                            self._attr_native_value = item[key]
                    else:
                        # Update daily data
                        daily_history = {}
                        for date in [day_before_yesterday, yesterday, today]:
                            end_date = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                            daily_data = await _async_daily_history(session, self._token, station_id, base_url, date, end_date)
                            if daily_data:
                                daily_history[date] = daily_data[0]
                                _LOGGER.debug("Daily data for %s, station_id %s: %s", date, station_id, daily_data[0])
                            else:
                                _LOGGER.warning("No daily data for %s, station_id %s", date, station_id)

                        relative_days = {
                            today: "_today",
                            yesterday: "_yesterday",
                            day_before_yesterday: "_day_before_yesterday"
                        }
                        relative_day = "_" + self._attr_unique_id.split("_")[-1].replace("_", "")
                        for date, data in daily_history.items():
                            if relative_day == relative_days.get(date):
                                key = self._attr_unique_id.split(f"{station_id}_")[1].split(relative_day)[0]
                                if data.get(key) is not None:
                                    self._attr_native_value = data[key]
                                    self._attr_extra_state_attributes = {"date": date, "station_id": station_id}
                                    break

        except Exception as exc:
            _LOGGER.error("Error updating sensor %s: %s", self._attr_unique_id, exc)
            self._attr_native_value = None
            self._attr_extra_state_attributes["last_update_error"] = str(exc)

async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
):
    _LOGGER.info("Bắt đầu thiết lập tích hợp DeyeCloud")
    global HISTORY_START_MONTH
    HISTORY_START_MONTH = entry.data.get(CONF_START_MONTH, "2024-01")
    _LOGGER.debug("HISTORY_START_MONTH được thiết lập: %s", HISTORY_START_MONTH)

    username = entry.data.get(CONF_USERNAME)
    password = entry.data.get(CONF_PASSWORD)
    app_id = entry.data.get(CONF_APP_ID)
    app_secret = entry.data.get(CONF_APP_SECRET)
    base_url = entry.data.get(CONF_BASE_URL)
    _LOGGER.debug("Thông tin cấu hình: username=%s, app_id=%s, base_url=%s", username, app_id, base_url)

    await _async_clear_cache(hass)
    await _async_clear_daily_cache(hass)

    entities: list[SensorEntity] = []
    cache: dict[str, list[dict]] = {}
    daily_cache: dict[str, dict] = {}

    try:
        async with aiohttp.ClientSession() as session:
            _LOGGER.debug("Đang khởi tạo phiên API")
            token = await _async_get_token(session, username, password, app_id, app_secret, base_url)
            stations = await _async_station_list(session, token, base_url)

            if not stations:
                _LOGGER.warning("Không nhận được trạm nào từ API")
                return True

            now = datetime.now()
            this_year, this_month = now.year, now.month
            last_month_dt = now - relativedelta(months=1)
            prev_year, prev_month = last_month_dt.year, last_month_dt.month

            today = now.strftime("%Y-%m-%d")
            yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
            day_before_yesterday = (now - timedelta(days=2)).strftime("%Y-%m-%d")

            _METRICS = [
                ("Solar Generation", "generationValue"),
                ("Monthly Consumption", "consumptionValue"),
                ("Monthly Grid Export", "gridValue"),
                ("Monthly Grid Import", "purchaseValue"),
                ("Monthly Battery Charge", "chargeValue"),
                ("Monthly Battery Discharge", "dischargeValue"),
            ]

            _DAILY_METRICS = [
                ("Solar Generation", "generationValue"),
                ("Daily Consumption", "consumptionValue"),
                ("Daily Grid Export", "gridValue"),
                ("Daily Grid Import", "purchaseValue"),
                ("Daily Battery Charge", "chargeValue"),
                ("Daily Battery Discharge", "dischargeValue"),
            ]

            # Process each station concurrently
            async def process_station(st):
                station_id = st.get("id") or st.get("stationId")
                if not station_id:
                    _LOGGER.warning("Bỏ qua trạm không có ID: %s", st)
                    return []

                _LOGGER.info("Xử lý trạm: station_id=%s", station_id)

                # Fetch monthly data
                history = await _async_history(session, token, station_id, base_url)
                cache[str(station_id)] = history

                history_index = {(i["year"], i["month"]): i for i in history}

                station_entities = []

                # Create monthly sensors
                for item in sorted(history, key=lambda x: (x["year"], x["month"])):
                    gen_val = item.get("generationValue")
                    if gen_val is None:
                        continue
                    y = item["year"]
                    m = f"{item['month']:02d}"
                    month_name = datetime(year=int(y), month=int(m), day=1).strftime("%b %Y")
                    name = f"Deye {station_id} {month_name}"
                    uid = f"{station_id}_raw_{y}_{m}"
                    _LOGGER.debug("Tạo sensor tháng: name=%s, unique_id=%s", name, uid)
                    station_entities.append(
                        _BasicSensor(
                            name=name,
                            unique_id=uid,
                            native_value=gen_val,
                            unit="kWh",
                            device_class="energy",
                            state_class="total_increasing",
                            extra_state_attributes=item,
                            hass=hass,
                            entry=entry,
                        )
                    )

                for (nice_name, key) in _METRICS:
                    cur_item = history_index.get((this_year, this_month), {})
                    cur_val = cur_item.get(key)
                    if cur_val is not None:
                        name = f"{nice_name} {station_id}"
                        uid = f"{station_id}_{key}_current_month"
                        _LOGGER.debug("Tạo sensor tháng hiện tại: name=%s, unique_id=%s", name, uid)
                        station_entities.append(
                            _BasicSensor(
                                name=name,
                                unique_id=uid,
                                native_value=cur_val,
                                unit="kWh",
                                device_class="energy",
                                state_class="total_increasing",
                                extra_state_attributes={
                                    "year": this_year,
                                    "month": this_month,
                                    "station_id": station_id,
                                },
                                hass=hass,
                                entry=entry,
                            )
                        )

                    prev_item = history_index.get((prev_year, prev_month), {})
                    prev_val = prev_item.get(key)
                    if prev_val is not None:
                        name = f"{nice_name} (Tháng trước) {station_id}"
                        uid = f"{station_id}_{key}_last_month"
                        _LOGGER.debug("Tạo sensor tháng trước: name=%s, unique_id=%s", name, uid)
                        station_entities.append(
                            _BasicSensor(
                                name=name,
                                unique_id=uid,
                                native_value=prev_val,
                                unit="kWh",
                                device_class="energy",
                                state_class="total_increasing",
                                extra_state_attributes={
                                    "year": prev_year,
                                    "month": prev_month,
                                    "station_id": station_id,
                                },
                                hass=hass,
                                entry=entry,
                            )
                        )

                # Fetch daily data
                daily_history = {}
                for date in [day_before_yesterday, yesterday, today]:
                    end_date = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                    daily_data = await _async_daily_history(session, token, station_id, base_url, date, end_date)
                    if daily_data:
                        daily_history[date] = daily_data[0]
                        _LOGGER.debug("Daily data for %s, station_id %s: %s", date, station_id, daily_data[0])
                    else:
                        _LOGGER.warning("No daily data for %s, station_id %s", date, station_id)

                daily_cache[str(station_id)] = daily_history

                # Create daily sensors with relative names
                relative_days = {
                    today: "_today",
                    yesterday: "_yesterday",
                    day_before_yesterday: "_day_before_yesterday"
                }
                for date, data in daily_history.items():
                    relative_day = relative_days.get(date, date.replace('-', '_'))
                    for (nice_name, key) in _DAILY_METRICS:
                        value = data.get(key)
                        if value is not None:
                            name = f"{nice_name} {relative_day} {station_id}"
                            uid = f"{station_id}_{key}{relative_day}"
                            _LOGGER.debug("Tạo sensor ngày: name=%s, unique_id=%s", name, uid)
                            station_entities.append(
                                _BasicSensor(
                                    name=name,
                                    unique_id=uid,
                                    native_value=value,
                                    unit="kWh",
                                    device_class="energy",
                                    state_class="total_increasing",
                                    extra_state_attributes={"date": date, "station_id": station_id},
                                    hass=hass,
                                    entry=entry,
                                )
                            )

                return station_entities

            # Process all stations concurrently
            all_entities = await asyncio.gather(*(process_station(st) for st in stations), return_exceptions=True)
            for result in all_entities:
                if isinstance(result, Exception):
                    _LOGGER.error("Lỗi khi xử lý một trạm: %s", result)
                else:
                    entities.extend(result)

            # Fetch device list and create status sensors
            device_sns = await _async_get_device_list(session, token, base_url, stations)
            if device_sns:
                device_data_list = await _async_get_device_status(session, token, base_url, device_sns[:10])  # Limit to 10 devices
                for device_data in device_data_list:
                    device_sn = device_data.get("deviceSn")
                    if device_sn:
                        for data in device_data.get("dataList", []):
                            key = data["key"]
                            value = data["value"]
                            unit = data["unit"]
                            name = f"{key} {device_sn}"
                            uid = f"device_{device_sn}_{key}"
                            _LOGGER.debug("Tạo sensor trạng thái: name=%s, unique_id=%s", name, uid)
                            entities.append(
                                _BasicSensor(
                                    name=name,
                                    unique_id=uid,
                                    native_value=value,
                                    unit=unit,
                                    hass=hass,
                                    entry=entry,
                                    extra_state_attributes={
                                        "device_sn": device_sn,
                                        "device_type": device_data.get("deviceType"),
                                        "device_state": device_data.get("deviceState"),
                                        "collection_time": device_data.get("collectionTime"),
                                    }
                                )
                            )

        # Save caches
        _LOGGER.debug("Chuẩn bị lưu cache: monthly_cache=%d station_id, daily_cache=%d station_id",
                      len(cache), len(daily_cache))
        await _async_save_cache(hass, cache)
        await _async_save_daily_cache(hass, daily_cache)
        _LOGGER.debug("Chuẩn bị thêm %d entities", len(entities))
        async_add_entities(entities, update_before_add=True)
        _LOGGER.info("Hoàn tất thiết lập tích hợp DeyeCloud")

    except Exception as exc:
        _LOGGER.error("Lỗi khi thiết lập tích hợp DeyeCloud: %s", exc)
        raise

    return True