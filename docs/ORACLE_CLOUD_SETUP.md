# Hosting F&O Relative Volume Scanner on Oracle Cloud (Always Free)

Complete step-by-step reference for running this scanner persistently on Oracle Cloud Infrastructure (OCI) Always Free in an Indian region (**Mumbai** or **Hyderabad**), including cost rules, ARM64 setup, `systemd`, HTTPS/auth, and maintenance.

---

## 1. Cost Confirmation (Is It Truly ₹0?)

**Yes — ₹0/month** as long as you stay strictly inside OCI's **Always Free** limits and deploy inside your **Home Region**:

| Resource | Always Free Allowance | Needed for This Scanner |
| :--- | :--- | :--- |
| **Compute (ARM)** | `VM.Standard.A1.Flex`: `1,500 OCPU hrs` + `9,000 GB hrs` / month (e.g., **2 OCPU + 4–12 GB RAM** `24×7`) | **1 VM: 2 OCPU + 4 GB (or 6 GB) RAM** |
| **Block Storage** | Up to **200 GB** total (boot + data volumes combined) | **1 Boot Volume (50 GB default)** |
| **Public IPv4** | Ephemeral Public IPv4 attached to eligible free VM | **1 Ephemeral Public IPv4** *(or none if using Cloudflare Tunnel)* |
| **Outbound Bandwidth** | **10 TB / month** egress free | Typically **< 5 GB / month** |

### Card Verification & Billing Rules (Important for India)
1. **Temporary card hold at sign-up:** Oracle performs a temporary authorization hold of **~$1 USD (~₹85)** on Free Tier sign-up (or **~$100 USD** only if you later upgrade to Pay-As-You-Go). It is **not charged** and auto-reverses in **3–7 business days**. Use a credit card (Visa/Mastercard/Amex) with **international online transactions enabled** (debit/virtual cards often fail).
2. **After the 30-day $300 Free Trial expires:**
   * If you stay on **Free Tier** (do not upgrade to Pay-As-You-Go), Oracle **cannot bill your card** — Always Free resources keep running for ₹0, and any paid trial resources are stopped/reclaimed automatically.
   * If you voluntarily upgrade to **Pay-As-You-Go (PAYG)** to bypass `"Out of host capacity"` errors in Mumbai, Always Free resources still cost **₹0**, *provided every item stays within Always Free quotas*.
3. **Avoid accidental paid add-ons during VM creation:**
   * Create compute **only in your Home Region** (`ap-mumbai-1` or `ap-hyderabad-1`).
   * Verify the **`Always-Free-eligible`** badge is shown on Shape and Boot Volume.
   * Keep total Block Storage `<= 200 GB` and do **not** add paid Load Balancers, extra IPs, or paid backup policies.
   * *(Recommended on PAYG)* Set an **OCI Budget Alert** (`Billing & Cost Management → Budgets`) for `$1` (`~₹85`) so you get an email immediately if anything ever incurs a cent.

---

## 2. Sign-Up & Create the VM

1. **Choose Home Region permanently at sign-up:** Pick **India West (Mumbai — `ap-mumbai-1`)** or **India South (Hyderabad — `ap-hyderabad-1`)**. Always Free compute can **only** be created in this Home Region.
2. Go to **Compute → Instances → Create Instance**:
   * **Name:** `fno-volume-scanner`
   * **Placement:** Same Home Region Availability Domain
   * **Image:** **Ubuntu 24.04 Minimal aarch64** (or **Ubuntu 22.04 Minimal aarch64**)
   * **Shape:** **Ampere** → `VM.Standard.A1.Flex` → **2 OCPUs, 4 GB or 6 GB RAM**
     *(Tip: Avoid over-allocating 12 GB RAM if unused — keeping VM memory usage above 20% helps avoid OCI's 7-day idle reclamation rule described in Section 6.)*
     *(If Mumbai says `"Out of host capacity"`, try another Availability Domain, `1 OCPU / 4 GB`, or a PAYG upgrade while keeping Always Free shape.)*
   * **Networking:** Assign a **Public IPv4 address** cleanly inside a VCN/subnet.
   * **SSH Key:** Generate/save the `.key` private key safely (needed for `ssh` access).
   * **Boot Volume:** Keep default **47–50 GB** (`Always-Free-eligible`).

---

## 3. Server Setup (Ubuntu ARM64)

SSH into your VM from PowerShell / terminal:

```bash
chmod 400 ~/Downloads/oci-scanner.key   # Linux/macOS
ssh -i ~/Downloads/oci-scanner.key ubuntu@<YOUR_PUBLIC_IP>
```

Install system packages and Python environment:

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git python3 python3-venv python3-pip build-essential libffi-dev python3-dev

# Clone your repository
git clone <YOUR_REPO_URL> ~/volume_scanner_web
cd ~/volume_scanner_web

# Create venv & upgrade pip first so Linux aarch64 binary wheels (curl_cffi, orjson, pyarrow) resolve cleanly
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt

# Verify all tests pass on the VM
python -m unittest discover -s tests -p "test_*.py" -v
```

*(Optional quick check that `orjson` and `curl_cffi` imported cleanly without compilation errors:)*

```bash
python -c "import streamlit, curl_cffi, orjson, pandas, numpy; print('OK:', streamlit.__version__, orjson.__version__)"
```

---

## 4. Run 24×7 with `systemd`

Create a dedicated service so the scanner starts on boot and restarts automatically on crashes:

```bash
sudo tee /etc/systemd/system/fno-scanner.service > /dev/null <<'EOF'
[Unit]
Description=F&O Relative Volume Scanner (Streamlit)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/volume_scanner_web
ExecStart=/home/ubuntu/volume_scanner_web/venv/bin/streamlit run app.py --server.address 127.0.0.1 --server.port 8501 --server.headless true
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now fno-scanner
sudo systemctl status fno-scanner
```

View live logs anytime:

```bash
journalctl -u fno-scanner -f
```

---

## 5. Secure Access (HTTPS + Password — Pick One)

Because the scanner binds to `127.0.0.1:8501` above, strangers on the internet cannot hammer your public IP or exhaust Yahoo/NSE rate limits. Expose it safely using either option below:

### Option A: Cloudflare Tunnel (Easiest — Zero Open Firewall Ports)
* Keeps all incoming ports (`8501`, `80`, `443`) **closed** in OCI and Ubuntu firewalls, gives automatic HTTPS (`https://scanner.yourdomain.com`), and lets you put **Cloudflare Access** (email OTP / Google login) in front for free.
1. Install `cloudflared` (`arm64` `.deb` from Cloudflare) on the VM.
2. Point the tunnel hostname to `http://localhost:8501` in Cloudflare Zero Trust Dashboard.

### Option B: Direct Domain + Caddy Reverse Proxy (HTTPS + Basic Auth)
If you want direct access without Cloudflare (requires opening ports `80` and `443` in **both** OCI VCN Security List and Ubuntu `iptables`/`ufw`):

1. **OCI Console:** `Networking → Virtual Cloud Networks → <VCN> → Security Lists → Add Ingress Rules` for `0.0.0.0/0` TCP ports `80` and `443`.
2. **VM Firewall (`iptables` on Oracle Ubuntu images):**
   ```bash
   sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 80 -j ACCEPT
   sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 443 -j ACCEPT
   sudo netfilter-persistent save
   ```
3. **Install Caddy** (`sudo apt install -y caddy`) and configure `/etc/caddy/Caddyfile` with `caddy hash-password` + `reverse_proxy 127.0.0.1:8501` (supports WebSockets required by Streamlit automatically).

*(For quick SSH-only personal testing with zero firewall setup, you can also just forward port `8501` locally: `ssh -N -L 8501:127.0.0.1:8501 -i ~/oci-scanner.key ubuntu@<YOUR_PUBLIC_IP>` and open `http://localhost:8501` on your laptop.)*

---

## 6. Maintenance & Known Operational Notes

* **Updating code after new commits:**
  ```bash
  cd ~/volume_scanner_web && git pull && sudo systemctl restart fno-scanner
  ```
* **Oracle 7-Day Idle Instance Reclamation Policy:**
  Oracle may stop/reclaim Always Free instances if **over 7 consecutive days**, 95th-percentile **CPU < 20%**, **Network < 20%**, AND (on `A1` Arm) **Memory < 20%** are *all* true at the same time. Sizing your `A1.Flex` VM sensibly (e.g., `2 OCPU / 4 GB` instead of `12 GB`) keeps active Python/Streamlit/cache memory cleanly above that 20% memory floor without needing artificial load.
* **NSE India (`core/oi.py`) on datacenter IPs:**
  OCI Mumbai/Hyderabad gives low latency to Indian markets, though NSE's Akamai WAF may still occasionally rate-limit datacenter IPs. When throttled, `core/oi.py` backs off safely (`60s → 600s`) and price/volume scanning continues uninterrupted.
* **Indian Broker API / WebSocket Secrets (Upstox v3 & 5paisa Xstream):**
  Store broker API keys and credentials in `~/.streamlit/secrets.toml` (or `.streamlit/secrets.toml`, keep out of git, `chmod 600`), or pass via `EnvironmentFile=` in `fno-scanner.service`. Never commit `.cache/` or `secrets.toml`.
