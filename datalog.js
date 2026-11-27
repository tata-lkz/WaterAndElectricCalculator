// datalog.js — เก็บประวัติค่าลงฐานข้อมูล Postgres (Neon / Supabase)
// ไม่ตั้ง DATABASE_URL = ปิดฟีเจอร์นี้ (ระบบทำงานเหมือนเดิม เก็บแค่ในหน่วยความจำ)
//   DATABASE_URL    connection string ของ Postgres
//   LOG_DAYS        เก็บย้อนหลังกี่วัน (90)  เกินแล้วลบอัตโนมัติ
//   LOG_FLUSH_MIN   บันทึกลงฐานข้อมูลทุกกี่นาที (15)  ยิ่งนาน Neon ยิ่งได้หลับ ประหยัดโควตาฟรี
//   LOG_ROLLUP_DAYS ข้อมูลเก่ากว่ากี่วันให้ย่อเป็นรายชั่วโมง (7)  ลดขนาดฐานข้อมูลราว 60 เท่า ใส่ 0 = ไม่ย่อ เก็บรายนาทีตลอด
// เก็บเป็นรายนาที: ค่าเฉลี่ย/ต่ำสุด/สูงสุด/ล่าสุด ของแต่ละค่า (ประมาณ 2-3 MB ต่อวัน ต่อ 15 ค่า)
const url = process.env.DATABASE_URL || '';
const DAYS = +process.env.LOG_DAYS > 0 ? +process.env.LOG_DAYS : 90;
const FLUSH_MS = (+process.env.LOG_FLUSH_MIN > 0 ? +process.env.LOG_FLUSH_MIN : 15) * 60e3;
const FLUSH_MIN = FLUSH_MS / 60e3;
const ROLL_RAW = process.env.LOG_ROLLUP_DAYS;
const ROLLUP_SET = ROLL_RAW !== undefined && ROLL_RAW !== '' && isFinite(+ROLL_RAW) ? Math.max(0, +ROLL_RAW) : 7;
const ROLLUP = ROLLUP_SET > 0 && ROLLUP_SET < DAYS ? ROLLUP_SET : 0;   // 0 = ปิด (หรือตั้งมากกว่า LOG_DAYS ซึ่งไม่มีความหมาย)
const TZ = process.env.TG_TZ || 'Asia/Bangkok';
//   DB_SSL_VERIFY   1 = ตรวจใบรับรอง SSL เต็มรูปแบบ, 0 = เข้ารหัสแต่ไม่ตรวจ (ค่าเริ่มต้น: ตรวจถ้าเป็น Neon, ที่อื่นไม่ตรวจเพราะบางเจ้าใช้ CA ของตัวเอง เช่น Supabase)
const VERIFY = process.env.DB_SSL_VERIFY ? process.env.DB_SSL_VERIFY === '1' : /neon\.tech/i.test(url);
if (url && !VERIFY && !/localhost|127\.0\.0\.1/.test(url)) console.warn('[db] เข้ารหัส SSL แต่ไม่ตรวจใบรับรอง (ตั้ง DB_SSL_VERIFY=1 เพื่อตรวจ)');

function build() {
  const { Pool } = require('pg');
  // ตัด sslmode ออกจาก URL เพราะ pg จะเอาค่าใน URL มาทับ option ssl ด้านล่าง (ทำให้เจอ error self-signed certificate)
  const cleanUrl = url.replace(/([?&])sslmode=[^&]*&?/i, '$1').replace(/[?&]$/, '');
  const pool = new Pool({
    connectionString: cleanUrl, max: 3, idleTimeoutMillis: 30000, connectionTimeoutMillis: 20000,
    ssl: /localhost|127\.0\.0\.1/.test(url) ? false : { rejectUnauthorized: VERIFY },
  });
  pool.on('error', e => console.error('[db] pool', e.message));
  const kids = new Map(), cur = new Map(); let queue = [], schemaOk = false, chain = Promise.resolve(), lastOk = 0, lastErr = '';

  async function schema() {
    if (schemaOk) return;
    await pool.query('create table if not exists log_keys(id serial primary key, name text unique not null)');
    await pool.query(`create table if not exists log_readings(
      kid int not null references log_keys(id) on delete cascade, t timestamptz not null,
      v_avg double precision not null, v_min double precision not null, v_max double precision not null, v_last double precision not null,
      primary key (kid, t))`);
    await pool.query('alter table log_readings add column if not exists v_n int not null default 1');   // จำนวนตัวอย่างในแถว (ไว้ถัวเฉลี่ยถ่วงน้ำหนัก)
    await pool.query('create index if not exists log_readings_t on log_readings(t)');   // ให้ลบข้อมูลเก่าเร็ว ไม่ต้องสแกนทั้งตาราง
    // ข้อมูลเก่าที่ย่อเป็นรายชั่วโมงแล้ว (mins = จำนวนแถวรายนาทีที่ถูกรวม)
    await pool.query(`create table if not exists log_hourly(
      kid int not null references log_keys(id) on delete cascade, t timestamptz not null,
      v_avg double precision not null, v_min double precision not null, v_max double precision not null, v_last double precision not null,
      v_n int not null default 1, mins int not null default 1, primary key (kid, t))`);
    await pool.query('create index if not exists log_hourly_t on log_hourly(t)');
    // มุมมองรวมสองตาราง: ฝั่งอ่านใช้ log_all ตัวเดียว ไม่ต้องรู้ว่าข้อมูลถูกย่อหรือยัง
    await pool.query(`create or replace view log_all as
      select kid, t, v_avg, v_min, v_max, v_last, v_n, 1 as mins from log_readings
      union all
      select kid, t, v_avg, v_min, v_max, v_last, v_n, mins from log_hourly`);
    schemaOk = true;
  }
  async function kidOf(name, create = true) {
    if (kids.has(name)) return kids.get(name);
    let r = await pool.query('select id from log_keys where name = $1', [name]);
    if (!r.rows.length && create) r = await pool.query('insert into log_keys(name) values ($1) on conflict (name) do update set name = excluded.name returning id', [name]);
    if (!r.rows.length) return 0;
    kids.set(name, r.rows[0].id); return r.rows[0].id;
  }

  // รวมค่าเป็นรายนาทีในหน่วยความจำ
  function add(k, t, v) {
    const m = Math.floor(t / 60000) * 60000; let b = cur.get(k);
    if (b && b.m !== m) { queue.push([k, b]); b = null; }
    if (!b) cur.set(k, b = { m, n: 0, s: 0, mn: v, mx: v, la: v });
    b.n++; b.s += v; if (v < b.mn) b.mn = v; if (v > b.mx) b.mx = v; b.la = v;
    if (queue.length > 60000) queue.splice(0, queue.length - 60000);     // ฐานข้อมูลล่มนาน: ทิ้งของเก่าสุด กันกินแรม
  }

  async function doFlush(all) {
    const nowMin = Math.floor(Date.now() / 60000) * 60000;
    for (const [k, b] of cur) if (all || b.m < nowMin) { queue.push([k, b]); cur.delete(k); }
    while (queue.length) {
      await schema();
      const take = queue.splice(0, 5000), mm = new Map();
      for (const [k, b] of take) {                                          // รวมแถวซ้ำ (ชื่อ+นาที) กัน INSERT ชนกันเอง
        const key = k + '|' + b.m, o = mm.get(key);
        if (!o) mm.set(key, { k, ...b }); else { o.n += b.n; o.s += b.s; o.mn = Math.min(o.mn, b.mn); o.mx = Math.max(o.mx, b.mx); o.la = b.la; }
      }
      const rows = [...mm.values()];
      try {
        const ids = []; for (const r of rows) ids.push(await kidOf(r.k));
        await pool.query(
          `insert into log_readings(kid, t, v_avg, v_min, v_max, v_last, v_n)
           select a, to_timestamp(b / 1000.0), c, d, e, f, g
           from unnest($1::int[], $2::float8[], $3::float8[], $4::float8[], $5::float8[], $6::float8[], $7::int[]) as x(a, b, c, d, e, f, g)
           on conflict (kid, t) do update set
             v_avg = (log_readings.v_avg * log_readings.v_n + excluded.v_avg * excluded.v_n) / (log_readings.v_n + excluded.v_n),
             v_n = log_readings.v_n + excluded.v_n,
             v_min = least(log_readings.v_min, excluded.v_min), v_max = greatest(log_readings.v_max, excluded.v_max), v_last = excluded.v_last`,
          [ids, rows.map(r => r.m), rows.map(r => r.s / r.n), rows.map(r => r.mn), rows.map(r => r.mx), rows.map(r => r.la), rows.map(r => r.n)]);
        lastOk = Date.now(); lastErr = '';
        console.log(`[db] บันทึก ${rows.length} แถว`);
      } catch (e) { queue.unshift(...take); throw e; }                      // ไม่สำเร็จ: เก็บไว้ลองใหม่รอบหน้า
    }
  }
  const flush = all => (chain = chain.then(() => doFlush(all)).catch(e => { lastErr = e.message; console.error('[db] บันทึกไม่สำเร็จ (จะลองใหม่):', e.message); }));

  // ย่อข้อมูลรายนาทีที่เก่ากว่า ROLLUP วัน เป็นรายชั่วโมง (ถ่วงน้ำหนักค่าเฉลี่ยตามจำนวนตัวอย่าง) แล้วลบแถวรายนาทีทิ้ง
  // ตัดที่ขอบชั่วโมงเต็มเสมอ จึงรวมครบทั้งชั่วโมงในรอบเดียว ทำใน transaction เดียว ถ้าพังกลางทางข้อมูลไม่หาย
  async function doRollup() {
    if (!ROLLUP) return;
    await schema();
    const cut = Math.floor(Date.now() / 3600e3) * 3600e3 - ROLLUP * 864e5;
    const c = await pool.connect();
    try {
      await c.query('begin');
      const a = await c.query(
        `insert into log_hourly(kid, t, v_avg, v_min, v_max, v_last, v_n, mins)
         select kid, h, sum(v_avg * v_n) / sum(v_n), min(v_min), max(v_max), (array_agg(v_last order by t desc))[1], sum(v_n)::int, count(*)::int
         from (select kid, t, v_avg, v_min, v_max, v_last, v_n, to_timestamp(floor(extract(epoch from t) / 3600) * 3600) as h
               from log_readings where t < to_timestamp($1 / 1000.0)) s
         group by kid, h
         on conflict (kid, t) do update set
           v_avg = (log_hourly.v_avg * log_hourly.v_n + excluded.v_avg * excluded.v_n) / (log_hourly.v_n + excluded.v_n),
           v_n = log_hourly.v_n + excluded.v_n, mins = log_hourly.mins + excluded.mins,
           v_min = least(log_hourly.v_min, excluded.v_min), v_max = greatest(log_hourly.v_max, excluded.v_max), v_last = excluded.v_last`, [cut]);
      const d = await c.query('delete from log_readings where t < to_timestamp($1 / 1000.0)', [cut]);
      await c.query('commit');
      if (d.rowCount) console.log(`[db] ย่อข้อมูลเก่ากว่า ${ROLLUP} วัน: ${d.rowCount} แถวรายนาที -> ${a.rowCount} แถวรายชั่วโมง`);
    } catch (e) { await c.query('rollback').catch(() => {}); throw e; }
    finally { c.release(); }
  }
  const rollup = () => (chain = chain.then(doRollup).catch(e => console.error('[db] ย่อข้อมูลเก่าไม่สำเร็จ (จะลองใหม่):', e.message)));   // เข้าคิวเดียวกับการบันทึก/ล้างข้อมูล ไม่ทำพร้อมกัน
  const purge = () => schema().then(async () => {
    for (const t of ['log_readings', 'log_hourly']) await pool.query(`delete from ${t} where t < now() - make_interval(days => $1::int)`, [DAYS]);
  }).catch(e => console.error('[db] ลบข้อมูลเก่า', e.message));
  const maintain = () => purge().then(rollup);

  // เริ่มระบบ: สร้างตาราง + โหลดประวัติย้อนหลังเข้ากราฟ (รีสตาร์ทแล้วกราฟไม่หาย)
  async function loadHistory(series, histMs, histMax, maxKeys) {
    await schema();
    const r = await pool.query(
      `select k.name, (extract(epoch from r.t) * 1000)::float8 as ms, r.v_last
       from log_all r join log_keys k on k.id = r.kid
       where r.t >= now() - make_interval(secs => $1::float8 / 1000) order by k.name, r.t`, [histMs]);
    const by = new Map();
    for (const x of r.rows) { let a = by.get(x.name); if (!a) by.set(x.name, a = []); a.push([+x.ms, +x.v_last]); }
    let skipped = 0;
    for (const [k, a] of by) {
      if (!series.has(k) && series.size >= maxKeys) { skipped++; continue; }      // ไม่ให้ประวัติใน DB ดันจำนวนค่าเกิน MAX_KEYS
      const c = series.get(k) || [], m = a.filter(p => !c.length || p[0] < c[0][0]).concat(c);
      series.set(k, m.length > histMax ? m.slice(-histMax) : m);
    }
    console.log(`[db] โหลดประวัติ ${r.rows.length} จุด จาก ${by.size - skipped} ค่า` + (skipped ? ` (ข้าม ${skipped} ค่า เพราะเกิน MAX_KEYS)` : ''));
  }
  // ฐานข้อมูลฟรี (Neon) อาจกำลังตื่น: ลองใหม่สูงสุด 4 ครั้ง ถ้ายังไม่ได้ก็ทำงานต่อ บันทึกข้อมูลใหม่ได้ตามปกติ
  async function start(series, histMs, histMax, maxKeys = Infinity) {
    setInterval(() => flush(false), FLUSH_MS);
    setInterval(maintain, 6 * 3600e3);
    for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => { flush(true).finally(() => process.exit(0)); });   // ดีพลอยใหม่/ปิดเครื่อง: บันทึกที่ค้างก่อน
    for (let i = 1; i <= 4; i++) {
      try { await loadHistory(series, histMs, histMax, maxKeys); break; }
      catch (e) {
        console.error(`[db] โหลดประวัติไม่สำเร็จ (ครั้งที่ ${i}/4):`, e.message);
        if (i < 4) await new Promise(r => setTimeout(r, 5000 * i));
      }
    }
    maintain();    // ไม่รอให้เสร็จ (ไม่ให้การย่อข้อมูลก้อนใหญ่ครั้งแรกมาหน่วงการเปิดเว็บ)
  }

  // กราฟย้อนหลังยาวๆ: ย่อเหลือไม่เกิน ~700 จุด (ค่าเฉลี่ยต่อช่วง)
  async function history(k, from, to, maxPts = 700) {
    const id = await kidOf(k, false); if (!id) return [];
    const now = Date.now(), min = now - DAYS * 864e5;
    to = Math.min(to || now, now + 60000); from = !from || from < min ? min : from;
    const b = Math.max(60, Math.ceil((to - from) / 1000 / maxPts / 60) * 60);
    const r = await pool.query(
      `select (floor(extract(epoch from t) / $4) * $4 * 1000)::float8 as ms, (sum(v_avg * v_n) / sum(v_n))::float8 as a
       from log_all where kid = $1 and t >= to_timestamp($2 / 1000.0) and t <= to_timestamp($3 / 1000.0)
       group by 1 order by 1`, [id, from, to, b]);
    return r.rows.map(x => [+x.ms, +x.a]);
  }

  // สรุปรายวัน/รายเดือน: ต่ำสุด สูงสุด เฉลี่ย และ "ใช้ไป" (ค่าล่าสุดของช่วงนี้ ลบค่าล่าสุดของช่วงก่อนหน้า — ใช้กับค่าสะสมอย่างพลังงาน)
  async function daily(k, by, days) {
    const id = await kidOf(k, false); if (!id) return [];
    const r = await pool.query(
      `with g as (
         select date_trunc($3::text, t at time zone $4::text)::date as d,
                min(v_min) as mn, max(v_max) as mx, sum(v_avg * v_n) / sum(v_n) as av,
                (array_agg(v_last order by t))[1] as fi, (array_agg(v_last order by t desc))[1] as la, sum(mins)::int as n
         from log_all where kid = $1 and t >= now() - make_interval(days => $2::int) group by 1)
       select d::text as d, mn, mx, av, fi, la, la - coalesce(lag(la) over (order by d), fi) as delta, n from g order by d`,
      [id, Math.min(Math.max(+days || 30, 1), DAYS), by === 'month' ? 'month' : 'day', TZ]);
    return r.rows.map(x => ({ d: x.d, mn: +x.mn, mx: +x.mx, av: +x.av, fi: +x.fi, la: +x.la, delta: x.delta == null ? null : +x.delta, n: x.n }));
  }
  // สรุปทุกค่าในช่วง N วัน (ต่ำสุด สูงสุด เฉลี่ย ล่าสุด จำนวนแถว ช่วงเวลาที่มีข้อมูล) สำหรับหน้า Datalog
  async function summary(days) {
    const d = Math.min(Math.max(Math.round(+days) || 7, 1), DAYS);
    await schema();
    const r = await pool.query(
      `with w as (select kid, t, v_min, v_max, v_avg, v_n, mins from log_all where t >= now() - make_interval(days => $1::int)),
            a as (select kid, min(v_min) as mn, max(v_max) as mx, sum(v_avg * v_n) / sum(v_n) as av, sum(mins)::int as n, min(t) as t0, max(t) as t1 from w group by kid)
       select k.name, a.mn, a.mx, a.av, a.n, (extract(epoch from a.t0) * 1000)::float8 as t0, (extract(epoch from a.t1) * 1000)::float8 as t1,
              (select x.v_last from log_all x where x.kid = a.kid and x.t = a.t1 limit 1) as la
       from a join log_keys k on k.id = a.kid order by k.name`, [d]);
    return r.rows.map(x => ({ k: x.name, mn: +x.mn, mx: +x.mx, av: +x.av, n: x.n, t0: +x.t0, t1: +x.t1, la: +x.la }));
  }
  // ดึงแถวดิบรายนาทีไว้ส่งออก CSV (จำกัดไม่เกิน 300,000 แถว)
  async function rows(k, days) {
    const id = await kidOf(k, false); if (!id) return [];
    const d = Math.min(Math.max(Math.round(+days) || 30, 1), DAYS);
    const r = await pool.query(
      `select (extract(epoch from t) * 1000)::float8 as ms, v_avg, v_min, v_max, v_last from log_all
       where kid = $1 and t >= now() - make_interval(days => $2::int) order by t limit 300000`, [id, d]);
    return r.rows.map(x => [+x.ms, +x.v_avg, +x.v_min, +x.v_max, +x.v_last]);
  }
  // ล้างข้อมูลย้อนหลัง: k = ชื่อค่าเดียว หรือ null = ทั้งหมด (รอรอบบันทึกที่กำลังทำอยู่ให้จบก่อน กันข้อมูลเก่าถูกเขียนกลับ)
  function reset(k) {
    const p = chain.then(async () => {
      if (k) { cur.delete(k); queue = queue.filter(x => x[0] !== k); } else { cur.clear(); queue = []; }
      await schema();
      if (k) { const id = await kidOf(k, false); if (id) { await pool.query('delete from log_readings where kid = $1', [id]); await pool.query('delete from log_hourly where kid = $1', [id]); } }
      else await pool.query('truncate table log_readings, log_hourly');
    });
    chain = p.catch(() => {});
    return p;
  }
  // บันทึกค่าที่ค้างในหน่วยความจำลงฐานข้อมูลทันที (ถ้าเขียนไม่สำเร็จ จะโยน error กลับไปให้ผู้เรียกรู้ ไม่แกล้งตอบว่าสำเร็จ)
  const flushNow = async () => { lastErr = ''; await flush(true); if (lastErr) throw new Error(lastErr); };
  const stat = () => ({ lastOk, lastErr, pending: queue.length + cur.size, rollup: ROLLUP });
  return { on: true, DAYS, FLUSH_MIN, ROLLUP, add, start, history, daily, summary, rows, reset, flushNow, stat };
}

module.exports = url ? build() : {
  on: false, DAYS, FLUSH_MIN, ROLLUP, add() {}, async start() {}, async history() { return []; }, async daily() { return []; },
  async summary() { return []; }, async rows() { return []; }, async reset() {}, async flushNow() {}, stat() { return { lastOk: 0, lastErr: '', pending: 0, rollup: ROLLUP }; },
};
