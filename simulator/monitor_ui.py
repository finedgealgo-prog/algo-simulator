from __future__ import annotations

from html import escape


def build_monitor_toggle_page(
    *,
    running: bool,
    title: str = "Simulator Monitor",
    status_text: str = "",
    detail_text: str = "",
    start_href: str,
    stop_href: str,
    status_href: str,
) -> str:
    next_href = stop_href if running else start_href
    next_label = "Stop Monitor" if running else "Start Monitor"
    state_label = "Running" if running else "Stopped"
    accent = "#22c55e" if running else "#ef4444"
    safe_title = escape(title)
    safe_status_text = escape(status_text or state_label)
    safe_detail_text = escape(detail_text or "")
    safe_start_href = escape(start_href, quote=True)
    safe_stop_href = escape(stop_href, quote=True)
    safe_status_href = escape(status_href, quote=True)
    safe_next_href = escape(next_href, quote=True)
    safe_next_label = escape(next_label)
    safe_state_label = escape(state_label)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{safe_title}</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background:
        radial-gradient(circle at top, rgba(56, 189, 248, 0.16), transparent 34%),
        linear-gradient(160deg, #07111f 0%, #0f172a 55%, #111827 100%);
      color: #e5eefb;
      font-family: "Segoe UI", Tahoma, sans-serif;
      padding: 20px;
    }}
    .card {{
      width: min(560px, calc(100vw - 32px));
      background: rgba(10, 19, 34, 0.95);
      border: 1px solid rgba(148, 163, 184, 0.18);
      border-radius: 24px;
      padding: 30px;
      box-shadow: 0 22px 70px rgba(0, 0, 0, 0.35);
      text-align: center;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      padding: 10px 16px;
      border-radius: 999px;
      background: rgba(15, 23, 42, 0.92);
      border: 1px solid rgba(148, 163, 184, 0.14);
      color: #cbd5e1;
      font-size: 12px;
      letter-spacing: 0.14em;
      text-transform: uppercase;
    }}
    .dot {{
      width: 12px;
      height: 12px;
      border-radius: 999px;
      background: {accent};
      box-shadow: 0 0 16px color-mix(in srgb, {accent} 70%, transparent);
    }}
    h1 {{
      margin: 18px 0 10px;
      font-size: 30px;
      color: #f8fafc;
    }}
    p {{
      margin: 0 auto;
      max-width: 450px;
      color: #94a3b8;
      line-height: 1.7;
      font-size: 15px;
    }}
    .detail {{
      margin-top: 14px;
      color: #7dd3fc;
      font-size: 13px;
      word-break: break-word;
    }}
    .actions {{
      margin-top: 28px;
      display: flex;
      justify-content: center;
      gap: 12px;
      flex-wrap: wrap;
    }}
    .btn {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 180px;
      padding: 15px 22px;
      border-radius: 18px;
      text-decoration: none;
      border: none;
      cursor: pointer;
      font-size: 16px;
      font-weight: 700;
    }}
    .btn.primary {{
      background: linear-gradient(135deg, #38bdf8, #2563eb);
      color: #eff6ff;
      box-shadow: 0 16px 32px rgba(37, 99, 235, 0.24);
    }}
    .btn.secondary {{
      background: #1e293b;
      color: #cbd5e1;
      border: 1px solid rgba(148, 163, 184, 0.18);
    }}
  </style>
</head>
<body>
  <div class="card">
    <div class="badge"><span class="dot"></span>{safe_state_label}</div>
    <h1>{safe_title}</h1>
    <p>{safe_status_text}</p>
    <div class="detail">{safe_detail_text}</div>
    <div class="actions">
      <a class="btn primary" href="{safe_next_href}">{safe_next_label}</a>
      <a class="btn secondary" href="{safe_status_href}">View Status JSON</a>
    </div>
    <div class="actions" style="margin-top:12px;">
      <a class="btn secondary" href="{safe_start_href}">Open Start Page</a>
      <a class="btn secondary" href="{safe_stop_href}">Open Stop Page</a>
    </div>
  </div>
</body>
</html>"""
