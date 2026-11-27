// Service worker ของ MQTT Viewer — มีไว้ให้ติดตั้งเป็นแอปได้ และแสดงหน้า "ต่อไม่ได้" แทนหน้าขาวของเบราว์เซอร์
// จงใจ "ไม่แคช" อะไรเลย: ข้อมูลเป็นแบบสดและอยู่หลังรหัสผ่าน แคชไว้จะเสี่ยงโชว์ค่าเก่า/หน้าของคนที่ล็อกอินไว้ก่อน
// (WebSocket และ /api/* ไม่ผ่านตัวนี้ ทำงานตามปกติ)
const OFFLINE = `<!doctype html><html lang="th"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>เชื่อมต่อไม่ได้ · MQTT Viewer</title>
<style>:root{color-scheme:light dark;font-family:Sarabun,"Noto Sans Thai",system-ui,sans-serif}
body{margin:0;min-height:100vh;display:grid;place-items:center;padding:20px;background:#edf2f5;color:#14303c;text-align:center}
@media(prefers-color-scheme:dark){body{background:#060a12;color:#e8f6ff}}
div{max-width:340px}h1{font-size:22px;margin:0 0 8px}p{margin:0 0 18px;line-height:1.6;opacity:.8}
button{font:inherit;font-weight:600;padding:11px 22px;border-radius:10px;border:0;background:#0e7490;color:#fff;cursor:pointer}</style></head>
<body><div><h1>เชื่อมต่อเซิร์ฟเวอร์ไม่ได้</h1>
<p>ตรวจสอบอินเทอร์เน็ตของเครื่อง หรือถ้าเซิร์ฟเวอร์เพิ่งถูกปลุก (แพ็กเกจฟรี) รอประมาณ 1 นาทีแล้วลองใหม่</p>
<button onclick="location.reload()">ลองใหม่</button></div></body></html>`;

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));
self.addEventListener('fetch', e => {
  if (e.request.mode !== 'navigate') return;     // เฉพาะการเปิดหน้า; อย่างอื่นปล่อยผ่านตามปกติ
  e.respondWith(fetch(e.request).catch(() => new Response(OFFLINE, { status: 503, headers: { 'Content-Type': 'text/html; charset=utf-8' } })));
});
