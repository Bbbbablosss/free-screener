# Payments (Heleket)

Crypto checkout for CRYPTO PRO. Isolated, additive module — nothing here is
imported by `main.py` at module load; the two HTTP routes are injected by
`backend/patch_payments.py`, and the frontend button is wired by
`backend/patch_payments_ui.py`.

## Flow

1. User clicks **Get PRO** on `/premium` → `POST /api/pay/create`.
2. Backend resolves the buyer from the **session cookie** (never the request body),
   selects the server-owned price table for the validated UI locale (RU/UK or EN/ES),
   applies the selected term price and then any valid promo, writes a
   `pay_orders` row (server-side source of truth), and creates a Heleket invoice.
3. User is redirected to Heleket's hosted payment page and pays in crypto.
4. Heleket calls `POST /api/pay/heleket/callback`. The signature is verified with
   the API key; on `paid`/`paid_over` PRO is granted **exactly once** via the
   affiliate program's `record_purchase()` (so partner attribution/commissions
   keep working).
5. Heleket redirects the user back to `/premium?pay=success`; the SPA re-checks
   `/api/auth/me` a few times so PRO reflects without a manual refresh.

## Security

- **The API key never touches git, logs, or any HTTP response.** It is read from
  `HELEKET_API_KEY` at call time and used only to sign requests / verify webhooks.
  `heleket.py` keeps it private (no getter).
- **The webhook is not trusted for amount or buyer.** Those come from the stored
  order keyed on `order_id`; a forged/replayed callback can't grant PRO to an
  arbitrary account or for an arbitrary amount.
- **Signature check** is constant-time (`hmac.compare_digest`).
- **Idempotency**: `pay_orders.granted` is flipped 0→1 atomically; only the winner
  runs the grant. On grant failure it resets so Heleket's retry can succeed.
- **IP allowlist** (`HELEKET_WEBHOOK_IPS`) is defense-in-depth; enforced only when
  `HELEKET_STRICT_IP=1` (leave off unless nginx forwards the real client IP).

## Deploy (prod: web server .214)

```bash
# 1) Secret (root-only; NOT under git). Paste the real key/merchant here:
sudo install -m 600 -o root -g root /dev/null /etc/screener/payments.env
sudo nano /etc/screener/payments.env          # see heleket.env.example

# 2) Make systemd load it into the screener service:
sudo mkdir -p /etc/systemd/system/screener.service.d
sudo tee /etc/systemd/system/screener.service.d/payments.conf >/dev/null <<'EOF'
[Service]
EnvironmentFile=/etc/screener/payments.env
EOF
sudo systemctl daemon-reload

# 3) Inject the backend routes (idempotent, backs up main.py):
python /opt/screener/backend/patch_payments.py

# 4) Wire the frontend button (run AFTER patch_auth_ui.py):
python /opt/screener/backend/patch_auth_ui.py
python /opt/screener/backend/patch_payments_ui.py

# 5) Restart:
sudo systemctl restart screener
```

## Configure the webhook / callback

The invoice sends `url_callback = {PAY_PUBLIC_BASE_URL}/api/pay/heleket/callback`.
Make sure that path is reachable from the public internet (nginx → uvicorn) and
NOT behind the PRO `auth_request` gate. Optionally set the same URL as the default
callback in the Heleket dashboard.

## Test

- Use Heleket's **Test Webhook** method to fire a signed callback and confirm the
  service verifies the signature and grants PRO.
- `python backend/payments/selftest.py` runs an offline sign round-trip (uses a
  dummy key from the env; makes no network calls).
