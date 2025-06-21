
## 👤 Tác giả & Thông tin liên hệ

- **Tên**: Trần Công Tuấn Anh  
- **GitHub**: [@heavenknows1978](https://github.com/heavenknows1978)  
- **Email**: hk@homeviet.org
- **Dự án**: [hass-deyecloud](https://github.com/heavenknows1978/hass-deyecloud)  
- **License**: MIT

# 🌞 Fuuuuckyou Tony Tong Deye Cloud Home Assistant Integration

A custom integration to connect your Home Assistant with your Deye solar inverter via the official Deye Cloud API.

---

## 📥 Features

- 🟢 Fetch monthly data: generation, consumption, battery, grid import/export, fetching recent days information, fetching current device status
- 📈 Sensors for current & last month, today, yesterday...
- 🔃 Auto refresh every minute (no YAML needed)
- ✅ Clean and simple setup via UI

---

## 🛠 Installation

### Option 1: Manual

1. Download or clone this repository
2. Copy `custom_components/deyecloud/` into your `/config/custom_components/` directory in Home Assistant
3. Restart Home Assistant
4. Go to **Settings → Devices & Services → Add Integration → DeyeCloud**
5. Fill in your credentials and API details

### Option 2: Via HACS### Option 2: Via HACS
## 📦 Thêm Custom Repository vào HACS

Nếu bạn chưa từng thêm repository tùy chỉnh trong HACS, hãy làm theo hướng dẫn sau:

1. Truy cập **HACS → Integrations**
2. Nhấn vào nút menu ba chấm (⋮) góc trên bên phải → **Custom repositories**
3. Ở phần *Add custom repository*, nhập đường link sau:

   ```
   https://github.com/heavenknows1978/hass-deyecloud
   ```

4. Ở ô loại repository (**Category**), chọn: `Integration`
5. Nhấn **Add**. Repository sẽ được thêm vào HACS.
6. Quay lại HACS → Integrations → nhấn **Explore & Add Repositories**
7. Tìm kiếm `DeyeCloud` → nhấn **Install**
8. Khởi động lại Home Assistant
9. Vào **Settings → Devices & Services → Add Integration → DeyeCloud** để cấu hình


1. Go to HACS → Integrations → 3-dot menu → Custom repositories
2. Add: `https://github.com/heavenknows1978/hass-deyecloud` (as Integration)
3. Search for "DeyeCloud" in HACS Integrations and install
4. Restart Home Assistant and add via UI

---

## 🔐 Get your API Credentials

### Step 1 – Register developer account

👉 Go to: https://developer.deyecloud.com/home  
→ Register or login with your Deye Cloud credentials

### Step 2 – Create a new App

👉 Go to: https://developer.deyecloud.com/app  
→ Click **“Create App”**  
→ You'll get:

- `App ID`
- `App Secret`

Use these during integration setup.

### Step 3 – Choose correct Base URL

Depending on your region:

| Region | Base URL |
|--------|----------|
| 🇪🇺 Europe | `https://eu1-developer.deyecloud.com/v1.0` |
| 🇺🇸 US     | `https://us1-developer.deyecloud.com/v1.0` |

---

## ⚙️ Configuration Fields

| Field       | Description |
|-------------|-------------|
| Username    | Your Deye Cloud account (email) |
| Password    | Your Deye password |
| App ID      | From developer portal |
| App Secret  | From developer portal |
| Base URL    | Based on your region |
| Start Month | First month to fetch history from (e.g. `2024-01`) |

---

## 📸 Sample Dashboard

> Sample Lovelace dashboard tiles showing PV generation, consumption, battery usage, grid stats etc.

![Dashboard](https://raw.githubusercontent.com/heavenknows1978/hass-deyecloud/main/screenshot.png)

---

## 🧾 Troubleshooting

- Check **Settings → System → Logs** for errors
- Ensure you restarted HA after copying files
- Ensure `custom_components/deyecloud/` has correct permissions

---

## 📄 License

[MIT License](LICENSE)