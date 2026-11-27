// MQTT Viewer + กราฟย้อนหลัง + รีพอร์ต Telegram (ไม่ใช้ Node-RED)
//
// ตัวแปรหลัก : MQTT_URL, MQTT_USER, MQTT_PASS, MQTT_TOPICS, PORT, APP_PASS (ไม่ตั้ง = ไม่ต้องล็อกอิน), APP_SECRET
// กราฟ       : HISTORY_HOURS (24)  เก็บย้อนหลังกี่ชั่วโมง
//              HISTORY_GAP_S (10)  เก็บจุดกราฟห่างกันอย่างน้อยกี่วินาที
//              HISTORY_MAX   (10000) จำนวนจุดสูงสุดต่อ 1 ค่า
//              MAX_KEYS      (500)   จำนวน topic / ชื่อค่าสูงสุดที่ยอมรับ (กันหน่วยความจำโตไม่จำกัด)
// ความปลอดภัย : ALLOW_NO_PASS=1  ยอมให้รันบน Render/production โดยไม่มี APP_PASS (ค่าเริ่มต้น: ไม่ยอม)
//              TRUST_PROXY_HOPS (1) จำนวนชั้นพร็อกซีหน้าแอป ใช้หา IP จริงของผู้ใช้สำหรับจำกัดการลองรหัสผ่าน
// ฐานข้อมูล   : DATABASE_URL, LOG_DAYS, LOG_FLUSH_MIN, LOG_ROLLUP_DAYS, DB_SSL_VERIFY (ดู datalog.js)
// Telegram   : TG_TOKEN   โทเคนจาก @BotFather
//              TG_CHAT    chat id ที่จะส่งรีพอร์ตให้ (คั่นหลายแชทด้วย ,)  ส่งข้อความ /id หาบอทเพื่อดูเลข
//              TG_TIMES   เวลาส่งรีพอร์ตอัตโนมัติ เช่น 08:00,18:00 (ค่าเริ่มต้น 08:00,18:00)
//              TG_TZ      เขตเวลา (Asia/Bangkok)
//              TG_HOURS   รีพอร์ตสรุปย้อนหลังกี่ชั่วโมง (24)
//              TG_CHARTS  จำนวนกราฟสูงสุดที่แนบต่อรีพอร์ต (8, ใส่ 0 = ไม่แนบกราฟ)
//              TG_POLL    ใส่ 0 เพื่อปิดการรับคำสั่ง /report จากแชท
// เฝ้าระวัง   : (ต้องตั้ง TG_TOKEN + TG_CHAT) แจ้ง Telegram เมื่ออุปกรณ์ไม่ส่งข้อมูล / broker หลุด และแจ้งอีกครั้งเมื่อกลับมา
//              WATCH_MIN        (5)  อุปกรณ์เงียบเกินกี่นาทีถึงแจ้ง (0 = ปิดการเฝ้าอุปกรณ์ทั้งหมด ยกเว้นที่ตั้งใน WATCH_RULES)
//              WATCH_RULES      กำหนดเวลาแยกตามชื่อ topic เช่น water=30,demo/data=3,test=0  (ชื่อมีคำนี้ = ใช้เวลานี้, 0 = ไม่เฝ้า)
//              WATCH_BROKER_S   (60) broker หลุดเกินกี่วินาทีถึงแจ้ง (0 = ปิด)
//              WATCH_REPEAT_MIN (0)  เตือนซ้ำทุกกี่นาทีถ้ายังไม่กลับมา (0 = แจ้งครั้งเดียว)
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto'), os = require('os');
const mqtt = require('mqtt');
const { WebSocketServer } = require('ws');
const datalog = require('./datalog');

process.on('unhandledRejection', e => console.error('[unhandledRejection]', e && e.message || e));

const num = (v, d) => (v !== undefined && v !== '' && isFinite(+v) ? +v : d);
const CFG = {
  url: process.env.MQTT_URL || '',
  username: process.env.MQTT_USER || '',
  password: process.env.MQTT_PASS || '',
  topics: (process.env.MQTT_TOPICS || 'Water/Meter/Value,demo/data/#').split(',').map(s => s.trim()).filter(Boolean),
  port: +process.env.PORT || 3100,
  appPass: process.env.APP_PASS || '',
  histMs: num(process.env.HISTORY_HOURS, 24) * 3600e3,
  histGap: num(process.env.HISTORY_GAP_S, 10) * 1000,
  histMax: num(process.env.HISTORY_MAX, 10000),
  maxKeys: num(process.env.MAX_KEYS, 500),
};
const TG = {
  token: process.env.TG_TOKEN || '',
  chats: (process.env.TG_CHAT || '').split(',').map(s => s.trim()).filter(Boolean),
  times: (process.env.TG_TIMES || '08:00,18:00').split(',').map(s => s.trim()).filter(s => /^\d{1,2}:\d{2}$/.test(s)).map(s => s.padStart(5, '0')),
  tz: process.env.TG_TZ || 'Asia/Bangkok',
  hours: num(process.env.TG_HOURS, 24),
  charts: num(process.env.TG_CHARTS, 8),
  poll: process.env.TG_POLL !== '0',
  only: (process.env.TG_ONLY || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean),   // ถ้าตั้ง: รีพอร์ตอัตโนมัติ/คำสั่ง /report ส่งเฉพาะค่าที่ชื่อมีคำเหล่านี้
};
TG.on = !!(TG.token && TG.chats.length);
if (!CFG.url) { console.error('ยังไม่ได้ตั้งค่า MQTT_URL (เช่น mqtt://host:1883)'); process.exit(1); }
if (!CFG.appPass) {
  // ไม่มีรหัสผ่าน = ใครก็เปิดดูค่าและสั่งส่งรีพอร์ต Telegram ได้ ใช้ได้เฉพาะในเครื่องตัวเอง
  if ((process.env.RENDER || process.env.NODE_ENV === 'production') && process.env.ALLOW_NO_PASS !== '1') {
    console.error('ไม่ได้ตั้งค่า APP_PASS บนเซิร์ฟเวอร์จริง เว็บจะเปิดให้ทุกคนเห็น จึงไม่ยอมเริ่มทำงาน\n'
      + 'ตั้ง APP_PASS ใน Environment (แนะนำ) หรือตั้ง ALLOW_NO_PASS=1 ถ้าตั้งใจให้เปิดสาธารณะ');
    process.exit(1);
  }
  console.warn('⚠️  ไม่ได้ตั้งค่า APP_PASS: ใครเข้าถึงเว็บนี้ได้ก็เห็นข้อมูลได้ (ใช้ได้เฉพาะในเครื่องตัวเอง)');
}

// ---------- ล็อกอิน (ถ้ามี APP_PASS) ----------
const TOKEN = crypto.createHmac('sha256', process.env.APP_SECRET || CFG.appPass || 'x').update('mqtt-viewer-session').digest('hex');
const eq = (a, b) => { const A = Buffer.from(String(a)), B = Buffer.from(String(b)); return A.length === B.length && crypto.timingSafeEqual(A, B); };
const cookies = req => Object.fromEntries((req.headers.cookie || '').split(';').map(s => s.trim().split(/=(.*)/s).slice(0, 2)).filter(a => a[0]));
const authed = req => !CFG.appPass || eq(cookies(req).sid || '', TOKEN);
const fails = new Map();
// ค่าแรกของ x-forwarded-for ผู้ใช้ปลอมเองได้ ต้องนับจากท้าย (พร็อกซีของเราต่อ IP จริงไว้ท้ายสุด)
const HOPS = Math.max(1, num(process.env.TRUST_PROXY_HOPS, 1));
const ipOf = req => {
  const x = String(req.headers['x-forwarded-for'] || '').split(',').map(s => s.trim()).filter(Boolean);
  return x.length ? x[Math.max(0, x.length - HOPS)] : String(req.socket.remoteAddress || '');
};
const limited = ip => { const f = fails.get(ip); return f && f.n >= 8 && Date.now() - f.t < 600e3; };
const noteFail = ip => {
  const f = fails.get(ip);
  if (!f || Date.now() - f.t > 600e3) { if (fails.size >= 5000) fails.delete(fails.keys().next().value); fails.set(ip, { n: 1, t: Date.now() }); }
  else { f.n++; f.t = Date.now(); }
};
setInterval(() => { const now = Date.now(); for (const [ip, f] of fails) if (now - f.t > 600e3) fails.delete(ip); }, 600e3).unref();

// ---------- ข้อมูล + ประวัติสำหรับกราฟ ----------
const store = new Map();          // topic -> ข้อความล่าสุด
const series = new Map();         // ชื่อค่า -> [[เวลา, ค่า], ...]
const numericTopics = new Set();  // topic ที่มีค่าตัวเลข (ไว้แยกออกจากข้อความล้วน)
let state = { connected: false, error: '' };
const statusMsg = () => ({ kind: 'status', ...state, tg: TG.on, db: datalog.on, dbDays: datalog.DAYS, dbFlushMin: datalog.FLUSH_MIN, dbRollup: datalog.ROLLUP });

// ดึงค่าตัวเลขจาก payload: "12.5", "25.3°C" หรือ JSON {"flow":1.2,"temp":{"in":30}}
// ชื่อค่า = topic (ตัวเลขล้วน) หรือ "topic › field" (ค่าใน JSON)
const NUM = /^\s*([+-]?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?)(\s*)([A-Za-z°%³²\/µ]{0,10})\s*$/;
// "3 days" / "12 items" (เว้นวรรคแล้วตามด้วยคำตัวพิมพ์เล็กยาว) ถือเป็นข้อความ ไม่ใช่ค่าที่วัดได้ — ยกเว้นหน่วยจริงบางตัว
const UNIT_OK = new Set(['mbar', 'kvar', 'kvah', 'mmhg', 'inhg']);
const wordy = m => m[2] !== '' && /^[a-z]{4,}$/.test(m[3]) && !UNIT_OK.has(m[3]);
function numbers(topic, payload) {
  const m = NUM.exec(payload);
  if (m && !wordy(m)) return [{ k: topic, v: parseFloat(m[1]) }];
  let j; try { j = JSON.parse(payload); } catch { return []; }
  const out = [], seen = new Set();
  const sensors = j && Array.isArray(j.sensors) ? j.sensors : null;
  if (sensors) {      // รูปแบบอุปกรณ์ Thai PLC: ตั้งชื่อค่าเป็น "topic › อุปกรณ์ · ชื่อเซนเซอร์ (หน่วย)"
    const eqName = {};
    for (const e of Array.isArray(j.equipments) ? j.equipments : []) if (e && e.id) eqName[e.id] = e.name || e.id;
    for (const s of sensors) {
      if (!s || out.length >= 50) continue;
      // รับทั้งตัวเลขและสตริงตัวเลข เช่น "12.5"
      const v = typeof s.value === 'number' ? s.value : typeof s.value === 'string' && s.value.trim() !== '' ? Number(s.value) : NaN;
      if (!isFinite(v)) continue;
      let k = topic + ' › ' + (eqName[s.equipment_id] ? eqName[s.equipment_id] + ' · ' : '') + (s.name || s.id || '?') + (s.unit ? ' (' + s.unit + ')' : '');
      if (seen.has(k)) k += ' [' + s.id + ']';
      seen.add(k); out.push({ k, v });
    }
  }
  const walk = (o, p, d) => {
    if (typeof o === 'number' && isFinite(o)) out.push({ k: p, v: o });
    else if (d < 3 && o && typeof o === 'object') for (const [k, v] of Object.entries(o)) {
      if (out.length >= 50 || (sensors && d === 0 && k === 'sensors')) continue;
      walk(v, p + ' › ' + k, d + 1);
    }
  };
  walk(j, topic, 0);
  return out;
}
function record(k, t, v) {
  let a = series.get(k);
  if (!a) { if (series.size >= CFG.maxKeys) return; series.set(k, a = []); }
  const last = a[a.length - 1];
  if (last && t - last[0] < CFG.histGap) return;
  a.push([t, v]);
  const cut = t - CFG.histMs; let i = 0;
  while (i < a.length - 1 && a[i][0] < cut) i++;
  if (i) a.splice(0, i);
  if (a.length > CFG.histMax) a.splice(0, a.length - CFG.histMax);
}
// ส่งประวัติให้หน้าเว็บ (ลดจุดเหลือไม่เกิน max ต่อค่า เพื่อไม่ให้หนัก)
function historySnapshot(max = 600) {
  const h = {};
  for (const [k, a] of series) {
    if (a.length <= max) { h[k] = a; continue; }
    const step = a.length / max, o = [];
    for (let i = 0; i < max - 1; i++) o.push(a[Math.floor(i * step)]);
    o.push(a[a.length - 1]);
    h[k] = o;
  }
  return h;
}

// ---------- HTTP ----------
const MIME = { '.html': 'text/html; charset=utf-8', '.png': 'image/png', '.js': 'text/javascript; charset=utf-8', '.webmanifest': 'application/manifest+json' };
// ไฟล์ติดตั้งเป็นแอป (PWA) ต้องเปิดได้โดยไม่ล็อกอิน เพราะเบราว์เซอร์ดึง manifest/ไอคอนโดยไม่แนบ cookie
const PUBLIC_FILES = new Set(['/manifest.webmanifest', '/sw.js', '/icon-192.png', '/icon-512.png', '/icon-maskable-512.png', '/apple-touch-icon.png']);
function serve(res, file) {
  // ปกติไฟล์อยู่ในโฟลเดอร์ public/ ถ้าไม่พบจะลองหาข้างๆ server.js (กรณีวางไฟล์รวมกัน)
  const paths = [path.join(__dirname, 'public', file), path.join(__dirname, file)];
  const next = i => fs.readFile(paths[i], (e, b) => {
    if (e) { if (i + 1 < paths.length) return next(i + 1); res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream', 'Cache-Control': 'no-cache',
      'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'same-origin' });
    res.end(b);
  });
  next(0);
}
const redirect = (res, to, extra) => { res.writeHead(302, { Location: to, ...(extra || {}) }); res.end(); };
const json = (res, code, o) => { res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' }); res.end(JSON.stringify(o)); };
let lastManual = 0;
const readJson = (req, max = 4000) => new Promise(ok => {
  if (!String(req.headers['content-type'] || '').startsWith('application/json')) return ok(null);   // กันฟอร์มข้ามเว็บ (CSRF)
  let b = '';
  req.on('data', d => { b += d; if (b.length > max) req.destroy(); });
  req.on('end', () => { try { ok(JSON.parse(b || '{}')); } catch { ok(null); } });
  req.on('close', () => ok(null)); req.on('error', () => ok(null));
});
let csvFmt;
const csvTime = ms => (csvFmt = csvFmt || new Intl.DateTimeFormat('sv-SE', { timeZone: TG.tz, hourCycle: 'h23', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' })).format(ms);

const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/healthz') { res.writeHead(200); return res.end('ok'); }
  if (PUBLIC_FILES.has(u)) return serve(res, u.slice(1));
  if (u === '/login') {
    if (!CFG.appPass) return redirect(res, '/');
    if (req.method === 'POST') {
      const ip = ipOf(req);
      if (limited(ip)) { res.writeHead(429, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('ลองผิดหลายครั้งเกินไป รอ 10 นาทีแล้วลองใหม่'); }
      let body = '';
      req.on('data', d => { body += d; if (body.length > 2000) req.destroy(); });
      req.on('end', () => {
        const pw = new URLSearchParams(body).get('password') || '';
        if (eq(pw, CFG.appPass)) {
          fails.delete(ip);
          const secure = req.headers['x-forwarded-proto'] === 'https' ? '; Secure' : '';
          redirect(res, '/', { 'Set-Cookie': `sid=${TOKEN}; HttpOnly; Path=/; Max-Age=31536000; SameSite=Lax${secure}` });
        } else { noteFail(ip); redirect(res, '/login?e=1'); }
      });
      return;
    }
    return serve(res, 'login.html');
  }
  if (!authed(req)) {
    if (u === '/' || u === '/index.html' || u === '/mqttview') return redirect(res, '/login');
    res.writeHead(401); return res.end('unauthorized');
  }
  if (u === '/' || u === '/index.html' || u === '/mqttview') return serve(res, 'index.html');
  if (u === '/mqttdata') return json(res, 200, [...store.values()]);
  if ((u === '/api/history' || u === '/api/daily') && req.method === 'GET') {   // ข้อมูลย้อนหลังจากฐานข้อมูล
    const q = new URLSearchParams(req.url.split('?')[1] || ''), k = q.get('k') || '';
    if (!datalog.on) return json(res, 200, { ok: true, on: false, data: [] });
    if (!k || k.length > 400) return json(res, 400, { ok: false, msg: 'ไม่พบชื่อค่า' });
    const p = u === '/api/history'
      ? datalog.history(k, +q.get('from') || 0, +q.get('to') || 0)
      : datalog.daily(k, q.get('by'), +q.get('days') || 30);
    return p.then(data => json(res, 200, { ok: true, on: true, data }), e => { console.error('[db] query', e.message); json(res, 500, { ok: false, msg: 'อ่านฐานข้อมูลไม่สำเร็จ' }); });
  }
  if (u.startsWith('/api/log/')) {                                               // หน้า Datalog
    const q = new URLSearchParams(req.url.split('?')[1] || '');
    if (!datalog.on) return json(res, 200, { ok: false, on: false, data: [], msg: 'ยังไม่ได้เปิดการบันทึกถาวร (ตั้ง DATABASE_URL)' });
    const fail = e => { console.error('[db]', e.message); json(res, 500, { ok: false, msg: 'ฐานข้อมูลผิดพลาด ลองใหม่อีกครั้ง' }); };
    if (u === '/api/log/summary' && req.method === 'GET')
      return datalog.summary(+q.get('days') || 7).then(data => json(res, 200, { ok: true, on: true, data,
        info: { ...datalog.stat(), days: datalog.DAYS, flushMin: datalog.FLUSH_MIN, now: Date.now() } }), fail);
    if (u === '/api/log/export' && req.method === 'GET') {
      const k = q.get('k') || ''; if (!k || k.length > 400) return json(res, 400, { ok: false, msg: 'ไม่พบชื่อค่า' });
      return datalog.rows(k, +q.get('days') || 30).then(rows => {
        const out = ['﻿เวลา,เฉลี่ย,ต่ำสุด,สูงสุด,ล่าสุด'];
        for (const r of rows) out.push(csvTime(r[0]) + ',' + r.slice(1).join(','));
        const name = 'datalog-' + k.replace(/[^\w฀-๿.-]+/g, '_').slice(0, 80) + '.csv';
        res.writeHead(200, { 'Content-Type': 'text/csv; charset=utf-8', 'Content-Disposition': `attachment; filename="datalog.csv"; filename*=UTF-8''${encodeURIComponent(name)}` });
        res.end(out.join('\n'));
      }, fail);
    }
    if (u === '/api/log/flush' && req.method === 'POST')
      return datalog.flushNow().then(() => json(res, 200, { ok: true, msg: 'บันทึกลงฐานข้อมูลแล้ว', info: datalog.stat() }),
        e => { console.error('[db] flush', e.message); json(res, 500, { ok: false, msg: 'บันทึกไม่สำเร็จ (ข้อมูลยังค้างอยู่ในเซิร์ฟเวอร์และจะลองใหม่เอง): ' + e.message, info: datalog.stat() }); });
    if (u === '/api/log/reset' && req.method === 'POST')
      return readJson(req).then(o => {
        if (!o || (!o.all && (typeof o.k !== 'string' || !o.k))) return json(res, 400, { ok: false, msg: 'คำขอไม่ถูกต้อง' });
        const k = o.all ? null : o.k;
        return datalog.reset(k).then(() => {
          if (k) series.delete(k); else series.clear();                         // ล้างกราฟในหน่วยความจำด้วย ให้เริ่มนับใหม่
          bcast({ kind: 'reset', keys: k ? [k] : null });
          console.log('[db] ล้างข้อมูล', k || 'ทั้งหมด');
          json(res, 200, { ok: true, msg: k ? 'ล้างข้อมูลของค่านี้แล้ว' : 'ล้างข้อมูลทั้งหมดแล้ว' });
        });
      }).catch(fail);
    return json(res, 404, { ok: false });
  }
  if (u === '/api/report' && req.method === 'POST') {          // ปุ่ม "ส่งเข้า Telegram" ในหน้าเว็บ (ส่งเฉพาะค่าที่เลือก)
    let body = '';
    req.on('data', d => { body += d; if (body.length > 40000) req.destroy(); });
    return req.on('end', () => {
      if (Date.now() - lastManual < 15000) return json(res, 429, { ok: false, msg: 'รอสักครู่ แล้วค่อยกดใหม่' });
      let o = {}; try { o = JSON.parse(body || '{}'); } catch {}
      const keys = Array.isArray(o.keys) ? o.keys.filter(k => typeof k === 'string').slice(0, 100) : null;
      if (keys && !keys.length) return json(res, 400, { ok: false, msg: 'ยังไม่ได้เลือกค่าที่จะส่ง' });
      const maxH = CFG.histMs / 3600e3;
      const hours = o.hours === 0 ? maxH : Math.min(Math.max(+o.hours || TG.hours, 0.25), maxH);
      lastManual = Date.now();
      sendReport(TG.chats, hours, keys).then(
        n => json(res, 200, { ok: true, msg: `ส่งเข้า Telegram แล้ว (${keys ? keys.length + ' ค่า, ' : ''}แนบกราฟ ${n} รูป)` }),
        e => json(res, 500, { ok: false, msg: e.message }));
    });
  }
  res.writeHead(404); res.end('not found');
});

// ---------- WebSocket ไปหน้าเว็บ ----------
const wss = new WebSocketServer({ server, path: '/ws', verifyClient: ({ req }) => authed(req) });
const send = (ws, o) => ws.readyState === 1 && ws.send(JSON.stringify(o));
const bcast = o => wss.clients.forEach(c => send(c, o));
const snapshotMsg = () => ({ kind: 'snapshot', items: [...store.values()], history: historySnapshot(), gap: CFG.histGap, numeric: [...numericTopics], histMs: CFG.histMs, histMax: CFG.histMax });
wss.on('connection', ws => {
  ws.on('message', () => {});            // รับสัญญาณ heartbeat จากหน้าเว็บ (กันเซิร์ฟเวอร์ฟรีหลับ)
  ws.on('error', () => {});
  send(ws, statusMsg());
  send(ws, snapshotMsg());
});
setInterval(() => wss.clients.forEach(c => c.readyState === 1 && c.ping()), 25000);

// ---------- MQTT ----------
datalog.start(series, CFG.histMs, CFG.histMax, CFG.maxKeys).then(() => { if (datalog.on) bcast(snapshotMsg()); });   // โหลดเสร็จทีหลัง (ลองใหม่ได้) -> ส่งกราฟที่เติมประวัติแล้วให้ทุกหน้าที่เปิดอยู่   // โหลดประวัติจากฐานข้อมูลเข้ากราฟ (ถ้าไม่ตั้ง DATABASE_URL จะไม่ทำอะไร)
const opts = { reconnectPeriod: 3000, connectTimeout: 10000, keepalive: 60, protocolVersion: 4 };
if (CFG.username) opts.username = CFG.username;
if (CFG.password) opts.password = CFG.password;
const client = mqtt.connect(CFG.url, opts);
client.on('connect', () => {
  state = { connected: true, error: '' };
  wdUp();
  console.log('[mqtt] connected', CFG.url);
  client.subscribe(CFG.topics, (err, granted) => {
    if (err) state.error = 'subscribe: ' + err.message;
    else {
      const denied = (granted || []).filter(g => g.qos === 128).map(g => g.topic);
      if (denied.length) state.error = 'broker ปฏิเสธ topic: ' + denied.join(', ');
      else console.log('[mqtt] subscribed', CFG.topics.join(', '));
    }
    bcast(statusMsg());
  });
  bcast(statusMsg());
});
client.on('error', e => { state.error = e.message; console.error('[mqtt] error', e.message); bcast(statusMsg()); });
client.on('close', () => { if (state.connected) wdDown(); state.connected = false; bcast(statusMsg()); });
client.on('message', (topic, buf, packet) => {
  const prev = store.get(topic);
  // retained ที่ broker ส่งซ้ำหลังต่อใหม่ (ค่าเดิมเป๊ะ) ไม่ใช่ข้อมูลใหม่: ไม่บันทึกซ้ำ และไม่ทำให้ป้าย "สด" ขึ้นผิด
  if (prev && packet && packet.retain && prev.payload === buf.toString()) return;
  if (!prev && store.size >= CFG.maxKeys) return;   // topic ใหม่เกินเพดาน: ข้าม กันหน่วยความจำโต
  const m = { topic, payload: buf.toString(), t: Date.now() };
  store.set(topic, m);
  const pts = numbers(topic, m.payload);
  if (pts.length) numericTopics.add(topic);
  for (const p of pts) { record(p.k, m.t, p.v); if (series.has(p.k)) datalog.add(p.k, m.t, p.v); }   // datalog เก็บทุกค่า ไม่ผ่านตัวกรองช่วงห่าง histGap
  bcast({ kind: 'msg', ...m, pts });
});

// ---------- Telegram ----------
const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const fmt = v => String(+Number(v).toFixed(3));
const tf = (t, o) => new Intl.DateTimeFormat('en-GB', { timeZone: TG.tz, hourCycle: 'h23', ...o }).format(t);
const hhmm = t => tf(t, { hour: '2-digit', minute: '2-digit' });
const dmy = t => tf(t, { day: '2-digit', month: '2-digit', year: 'numeric' });
const ago = ms => { const s = Math.round(ms / 1000); return s < 90 ? s + ' วินาที' : s < 5400 ? Math.round(s / 60) + ' นาที' : (s / 3600).toFixed(1) + ' ชั่วโมง'; };

async function tg(method, body, form) {
  const r = await fetch(`https://api.telegram.org/bot${TG.token}/${method}`, {
    method: 'POST', headers: form ? undefined : { 'Content-Type': 'application/json' },
    body: form || JSON.stringify(body), signal: AbortSignal.timeout(40000),
  });
  const j = await r.json().catch(() => ({}));
  if (!j.ok) throw new Error(j.description || 'Telegram HTTP ' + r.status);
  return j.result;
}
async function sendText(chat, text) {
  const parts = []; let cur = '';
  for (const line of text.split('\n')) { if (cur.length + line.length > 3800) { parts.push(cur); cur = ''; } cur += line + '\n'; }
  if (cur.trim()) parts.push(cur);
  for (const p of parts) await tg('sendMessage', { chat_id: chat, text: p, parse_mode: 'HTML', disable_web_page_preview: true });
}
async function sendPhoto(chat, png, caption) {
  const f = new FormData();
  f.append('chat_id', chat); f.append('caption', caption); f.append('parse_mode', 'HTML');
  f.append('photo', new Blob([png], { type: 'image/png' }), 'chart.png');
  await tg('sendPhoto', null, f);
}

// ---------- เฝ้าระวังอุปกรณ์/broker ขาดการติดต่อ ----------
const WD = {
  mins: num(process.env.WATCH_MIN, 5),
  brokerMs: num(process.env.WATCH_BROKER_S, 60) * 1000,
  repeatMs: num(process.env.WATCH_REPEAT_MIN, 0) * 60e3,
  rules: (process.env.WATCH_RULES || '').split(',').map(x => x.trim().toLowerCase()).filter(Boolean)
    .map(x => { const i = x.lastIndexOf('='); return i > 0 && x.slice(i + 1).trim() !== '' && isFinite(+x.slice(i + 1)) ? { k: x.slice(0, i).trim(), m: +x.slice(i + 1) } : null; }).filter(Boolean),
};
const minsFor = topic => { const l = topic.toLowerCase(), r = WD.rules.find(x => l.includes(x.k)); return r ? r.m : WD.mins; };
let brokerUpAt = Date.now(), brokerDownSince = Date.now(), brokerAlerted = false;
const wdOff = new Map();                    // topic -> { last: เวลาข้อมูลล่าสุดตอนที่แจ้ง, r: เวลาที่แจ้ง/เตือนซ้ำครั้งล่าสุด }
function wdUp() { brokerUpAt = Date.now(); }
function wdDown() { brokerDownSince = Date.now(); }
async function wdSend(text) {
  const r = await Promise.allSettled(TG.chats.map(c => sendText(c, text)));
  if (!r.some(x => x.status === 'fulfilled')) throw new Error((r[0] && r[0].reason && r[0].reason.message) || 'ส่ง Telegram ไม่สำเร็จ');
}
let wdBusy = false;
async function wdTick() {
  if (!TG.on || wdBusy) return;
  wdBusy = true;
  try {
    const now = Date.now(), L = [], apply = [];
    if (WD.brokerMs > 0) {
      if (!state.connected && !brokerAlerted && now - brokerDownSince > WD.brokerMs) {
        L.push(`🔴 <b>เซิร์ฟเวอร์ต่อ MQTT broker ไม่ได้</b> มา ${ago(now - brokerDownSince)}แล้ว` + (state.error ? `\n  ${esc(state.error)}` : ''), '');
        apply.push(() => { brokerAlerted = true; });
      } else if (state.connected && brokerAlerted) {
        L.push(`🟢 <b>Broker กลับมาเชื่อมต่อแล้ว</b> (หลุดไป ${ago(now - brokerDownSince)})`, '');
        apply.push(() => { brokerAlerted = false; });
      }
    }
    // ตอน broker หลุดอยู่ ไม่ตัดสินว่าอุปกรณ์เงียบ (ข้อมูลไม่มาเพราะ broker); หลังต่อใหม่ให้เริ่มนับเวลาใหม่
    if (state.connected) {
      const downs = [], ups = [], rem = [];
      for (const [topic, m] of store) {
        const lim = minsFor(topic) * 60e3; if (!(lim > 0)) continue;
        const age = now - Math.max(m.t, brokerUpAt), o = wdOff.get(topic);
        if (age > lim) {
          if (!o) downs.push({ topic, m });
          else if (WD.repeatMs && now - o.r >= WD.repeatMs) rem.push({ topic, m, o });
        } else if (o && m.t > o.last) ups.push({ topic, m, o });    // ต้องมีข้อมูลใหม่จริงๆ ถึงนับว่ากลับมา (ไม่ใช่แค่ broker เพิ่งต่อใหม่)
      }
      const list = (title, a, line) => { if (!a.length) return; L.push(title + ` (${a.length})`); for (const x of a.slice(0, 30)) L.push(line(x)); if (a.length > 30) L.push(`  …และอีก ${a.length - 30} ค่า`); L.push(''); };
      list('🔴 <b>อุปกรณ์ไม่ส่งข้อมูล</b>', downs, x => `  • ${esc(x.topic)} — ข้อมูลล่าสุด ${hhmm(x.m.t)} (เงียบมา ${ago(now - x.m.t)})`);
      list('⏰ <b>ยังไม่กลับมา</b>', rem, x => `  • ${esc(x.topic)} — เงียบมา ${ago(now - x.m.t)}แล้ว`);
      list('🟢 <b>กลับมาส่งข้อมูลแล้ว</b>', ups, x => `  • ${esc(x.topic)} — หายไป ${ago(x.m.t - x.o.last)}`);
      for (const x of downs) apply.push(() => wdOff.set(x.topic, { last: x.m.t, r: now }));
      for (const x of rem) apply.push(() => { x.o.r = now; });
      for (const x of ups) apply.push(() => wdOff.delete(x.topic));
    }
    if (!apply.length) return;
    await wdSend(`🕒 ${dmy(now)} ${hhmm(now)}\n` + L.join('\n').trim());
    apply.forEach(f => f());                  // อัปเดตสถานะเมื่อส่งสำเร็จเท่านั้น ถ้าส่งไม่ได้จะลองใหม่รอบหน้า
    console.log(`[watch] แจ้งเตือนแล้ว (${apply.length} รายการ)`);
  } catch (e) { console.error('[watch] ส่งแจ้งเตือนไม่สำเร็จ (จะลองใหม่):', e.message); }
  finally { wdBusy = false; }
}
setInterval(wdTick, 15000);

function statusText() {
  const now = Date.now(), L = [`📡 Broker: ${state.connected ? 'เชื่อมต่อแล้ว ✅' : 'ขาดการเชื่อมต่อ ❌'}`, ''];
  const items = [...store.values()].sort((a, b) => a.topic.localeCompare(b.topic));
  for (const m of items) {
    const mark = wdOff.has(m.topic) ? '🔴' : minsFor(m.topic) > 0 ? '🟢' : '⚪';
    L.push(`${mark} ${esc(m.topic)} — ข้อมูลล่าสุด ${ago(now - m.t)}ก่อน`);
  }
  if (!items.length) L.push('ยังไม่มีข้อมูลจากอุปกรณ์');
  else L.push('', '🟢 ปกติ  🔴 ขาดการติดต่อ  ⚪ ไม่ได้เฝ้า');
  return L.join('\n');
}

// สรุปค่าต่ำสุด/สูงสุด/เฉลี่ย/เปลี่ยนแปลง ของแต่ละค่าในช่วงที่เลือก
function buildReport(hours, keys) {
  const now = Date.now(), since = now - hours * 3600e3;
  const sel = keys ? new Set(keys) : null;
  const pass = (k, topic) => sel ? sel.has(k) : !TG.only.length || TG.only.some(o => (topic || k).toLowerCase().includes(o));
  const L = [`📊 <b>รายงาน MQTT</b>${sel ? ` (เลือก ${sel.size} ค่า)` : ''}`, `🕒 ${dmy(now)} ${hhmm(now)} (ย้อนหลัง ${hours} ชม.)`,
    `📡 Broker: ${state.connected ? 'เชื่อมต่อแล้ว ✅' : 'ขาดการเชื่อมต่อ ❌'}`, ''];
  const charts = [];
  for (const k of [...series.keys()].sort()) {
    if (!pass(k)) continue;
    const a = series.get(k), w = a.filter(p => p[0] >= since);
    const lastT = a[a.length - 1][0];
    L.push(`<b>${esc(k)}</b>`);
    if (!w.length) { L.push(`  ไม่มีข้อมูลในช่วงนี้ (ล่าสุดเมื่อ ${ago(now - lastT)} ที่แล้ว)`, ''); continue; }
    let min = Infinity, max = -Infinity, sum = 0;
    for (const [, v] of w) { sum += v; if (v < min) min = v; if (v > max) max = v; }
    const first = w[0][1], last = w[w.length - 1][1], d = last - first;
    L.push(`  ล่าสุด <b>${fmt(last)}</b> (${hhmm(lastT)})`,
      `  ต่ำสุด ${fmt(min)} · สูงสุด ${fmt(max)} · เฉลี่ย ${fmt(sum / w.length)}`,
      `  เปลี่ยนแปลง ${d > 0 ? '+' : ''}${fmt(d)}  (${w.length} จุด)`);
    if (now - lastT > 15 * 60e3) L.push(`  ⚠️ ไม่มีข้อมูลใหม่มา ${ago(now - lastT)}แล้ว`);
    L.push('');
    if (w.length >= 2) charts.push({ k, pts: w });
  }
  const texts = [...store.values()].filter(m => !numericTopics.has(m.topic) && pass('t:' + m.topic, m.topic)).sort((a, b) => a.topic.localeCompare(b.topic));
  if (texts.length) { L.push('<b>ข้อความล่าสุด</b>'); for (const m of texts) L.push(`  ${esc(m.topic)}: ${esc(m.payload.slice(0, 80))}`); L.push(''); }
  if (!charts.length && !texts.length && !L.some(l => l.startsWith('<b>'))) L.push(sel ? 'ไม่พบค่าที่เลือก (ยังไม่มีข้อมูลจากอุปกรณ์)' : 'ยังไม่มีข้อมูลจาก MQTT');
  return { text: L.join('\n'), charts: charts.slice(0, sel ? 20 : TG.charts) };
}

// วาดกราฟเป็นรูป PNG ผ่าน quickchart.io (ไม่ต้องติดตั้งไลบรารีเพิ่ม)
async function chartPng(k, all) {
  const step = Math.max(1, Math.ceil(all.length / 120));
  const pts = all.filter((_, i) => i % step === 0 || i === all.length - 1);
  const long = pts[pts.length - 1][0] - pts[0][0] > 36 * 3600e3;
  const cfg = {
    type: 'line',
    data: { labels: pts.map(p => (long ? tf(p[0], { day: '2-digit', month: '2-digit' }) + ' ' : '') + hhmm(p[0])),
      datasets: [{ data: pts.map(p => p[1]), borderColor: '#0e8a9b', backgroundColor: 'rgba(14,138,155,.15)', fill: true, pointRadius: 0, borderWidth: 2, lineTension: 0.2 }] },
    options: { legend: { display: false }, title: { display: true, text: k, fontSize: 16 }, scales: { xAxes: [{ ticks: { maxTicksLimit: 8 } }] } },
  };
  const r = await fetch('https://quickchart.io/chart', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, signal: AbortSignal.timeout(20000),
    body: JSON.stringify({ version: '2', backgroundColor: 'white', width: 800, height: 400, format: 'png', chart: cfg }),
  });
  if (!r.ok) throw new Error('quickchart HTTP ' + r.status);
  return Buffer.from(await r.arrayBuffer());
}

let reporting = false;
async function sendReport(chats = TG.chats, hours = TG.hours, keys = null) {
  if (!TG.on) throw new Error('ยังไม่ได้ตั้งค่า TG_TOKEN / TG_CHAT');
  if (reporting) throw new Error('กำลังส่งรีพอร์ตอยู่ รอสักครู่');
  reporting = true;
  try {
    const { text, charts } = buildReport(hours, keys);
    const pngs = [];
    for (const c of charts) {
      try { pngs.push({ k: c.k, png: await chartPng(c.k, c.pts) }); } catch (e) { console.error('[tg] chart', c.k, e.message); }
    }
    for (const chat of chats) {
      await sendText(chat, text);
      for (const p of pngs) await sendPhoto(chat, p.png, `<b>${esc(p.k)}</b> · ย้อนหลัง ${hours} ชม.`);
    }
    console.log(`[tg] ส่งรีพอร์ตแล้ว → ${chats.length} แชท, กราฟ ${pngs.length} รูป`);
    return pngs.length;
  } finally { reporting = false; }
}

// ส่งอัตโนมัติตามเวลา TG_TIMES
let lastSlot = '';
setInterval(() => {
  if (!TG.on || !TG.times.length) return;
  const now = Date.now(), hm = hhmm(now), slot = dmy(now) + ' ' + hm;
  if (TG.times.includes(hm) && slot !== lastSlot) { lastSlot = slot; sendReport().catch(e => console.error('[tg] report', e.message)); }
}, 20000);

// รับคำสั่งจากแชท: /report [ชั่วโมง]  /status  /id  /help
async function pollTelegram() {
  let offset = 0;
  for (;;) {
    try {
      const ups = await tg('getUpdates', { offset, timeout: 25, allowed_updates: ['message'] });
      for (const u of ups) {
        offset = u.update_id + 1;
        const m = u.message; if (!m || !m.text) continue;
        const chat = String(m.chat.id), [cmd0, arg] = m.text.trim().split(/\s+/), cmd = cmd0.split('@')[0].toLowerCase();
        if (cmd === '/id') { await tg('sendMessage', { chat_id: chat, text: 'chat id ของแชทนี้: ' + chat }); continue; }
        if (!TG.chats.includes(chat)) continue;
        if (cmd === '/report') {
          const h = Math.min(Math.max(+arg || TG.hours, 1), CFG.histMs / 3600e3);
          sendReport([chat], h).catch(e => tg('sendMessage', { chat_id: chat, text: 'ส่งรีพอร์ตไม่สำเร็จ: ' + e.message }).catch(() => {}));
        } else if (cmd === '/status') {
          sendText(chat, statusText()).catch(() => {});
        } else if (cmd === '/start' || cmd === '/help') {
          await tg('sendMessage', { chat_id: chat, text: '/report — ขอรีพอร์ตตอนนี้\n/report 6 — สรุปย้อนหลัง 6 ชั่วโมง\n/status — ดูสถานะ broker และอุปกรณ์ทุกตัว\n/id — ดู chat id' });
        }
      }
    } catch (e) { console.error('[tg] poll', e.message); await new Promise(r => setTimeout(r, 10000)); }
  }
}
if (TG.token && TG.poll) pollTelegram();

server.listen(CFG.port, '0.0.0.0', () => {
  console.log(`เปิดในเครื่องนี้:  http://localhost:${CFG.port}` + (CFG.appPass ? '  (ต้องใส่รหัสผ่าน)' : ''));
  console.log(TG.on ? `[tg] เปิดใช้งาน ส่งรีพอร์ตเวลา ${TG.times.join(', ') || '(ไม่ตั้งเวลา)'} (${TG.tz})`
    : '[tg] ยังไม่เปิด (ตั้ง TG_TOKEN และ TG_CHAT เพื่อใช้งาน)');
  if (TG.on) console.log(`[watch] เฝ้าระวัง: อุปกรณ์เงียบเกิน ${WD.mins} นาที` + (WD.rules.length ? ` (กฎเฉพาะ ${WD.rules.map(r => r.k + '=' + r.m).join(', ')})` : '') + `, broker หลุดเกิน ${WD.brokerMs / 1000} วินาที` + (WD.repeatMs ? `, เตือนซ้ำทุก ${WD.repeatMs / 60e3} นาที` : ''));
  for (const list of Object.values(os.networkInterfaces()))
    for (const i of list || []) if (i.family === 'IPv4' && !i.internal)
      console.log(`เปิดจากมือถือ/เครื่องอื่น (Wi-Fi เดียวกัน):  http://${i.address}:${CFG.port}`);
});
