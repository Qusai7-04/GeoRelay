# GeoRelay — Secure Real-time Location Sharing Environment

GeoRelay is a full-stack, secure, concurrent real-time location sharing application. Authenticated users can stream live coordinates to a central backend using a fully-featured, sleek glassmorphism Web App, and authorized subscribers receive updates instantly over WebSockets.

## 🌟 Key Features

### 🖥️ Frontend Web Application
*   **Modern Interactive UI**: Stunning premium dark-mode glassmorphism aesthetics.
*   **Live Interactive Map**: Real-time plotting using Leaflet.js with dynamic subscription markers. 
*   **Three Modes of Operation**:
    *   **GPS Mode**: Stream your device's actual live geolocation data.
    *   **Click Mode**: Instantly jump your coordinates by clicking anywhere on the map.
    *   **Simulate Mode**: Automatically travel along programmed routes (e.g., Highway Drive, Zigzag, Random Walk).
*   **Resizable Activity Log**: A fully draggable, 8-way resizable window tracking network events and location packets.
*   **Built-in Web DB Terminal**: Features an interactive overlay simulating a `mysql` console to directly execute queries (like `show tables;` or `select * from Users;`) against the live backend SQLite database directly from your browser!

### ⚙️ Backend Architecture (Python & Websockets)
*   **Concurrent Connections**: Single-process asynchronous event-loop architecture (`asyncio`) for high-performance I/O bounding.
*   **Persistent Storage (SQLite)**: Logs users, sessions, live location points, and security audits. 
*   **Security Built-in**: PBKDF2 password hashing, session tokens, security audits, and per-message authorization gating.
*   **TCP/Socket Control**: Configured with `SO_REUSEADDR`, `SO_KEEPALIVE`, `SO_RCVBUF`, and `TCP_NODELAY` for network tuning.

---

## 🚀 Setup & Installation

**1. Create & Activate Virtual Environment:**
```bash
python -m venv .venv

# Windows
.venv\Scripts\activate
# Mac / Linux
source .venv/bin/activate
```

**2. Install Dependencies:**
```bash
pip install -r requirements.txt
```

---

## 🏃‍♂️ How to Run the Application

You will need to run two separate processes (one for the Backend API, one for the Frontend Web App).

**Step 1: Start the Backend WebSocket Server**
Open a terminal, activate your virtual environment, and run the following command:
```bash
python -m app.server \
  --host 0.0.0.0 \
  --port 8765 \
  --db-file location.db \
  --syslog-file server.log \
  --reuse-addr true \
  --keepalive true \
  --rcvbuf 262144 \
  --tcp-nodelay true
```

**Step 2: Start the Frontend HTTP Server**
Open a **new/second terminal**, navigate to the project root, and serve the `frontend` directory:
```bash
python -m http.server 8000 --directory frontend
```

**Step 3: Access the App!**
Open your web browser and navigate to:
👉 **[http://localhost:8000/](http://localhost:8000/)**

---

## 🛠️ Accessing the DB Terminal
Once you create an account and log in through the Web UI, look for the **Terminal** button in the top navigation bar. Opening this brings up the mock-MySQL DB interface. 

You can run these exact queries to inspect the backend persistence:
*   `show tables`
*   `select * from Users`
*   `select * from Sessions`
*   `select * from Locations`
*   `select * from Logs`
