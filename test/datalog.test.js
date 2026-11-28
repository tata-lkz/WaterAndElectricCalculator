'use strict';

const assert = require('node:assert/strict');
const Module = require('node:module');
const test = require('node:test');

const queries = [];
let pool;

class MockPool {
  constructor() {
    pool = this;
  }

  on() {}

  async query(text, params = []) {
    queries.push({ text, params });
    return { rows: [], rowCount: 0 };
  }
}

const originalLoad = Module._load;
const originalDatabaseUrl = process.env.DATABASE_URL;
const originalRawDays = process.env.LOG_RAW_DAYS;
process.env.DATABASE_URL = 'postgresql://test:test@localhost/test';
process.env.LOG_RAW_DAYS = '30';
Module._load = function (request, parent, isMain) {
  if (request === 'pg') return { Pool: MockPool };
  return originalLoad.call(this, request, parent, isMain);
};
const datalog = require('../datalog');
Module._load = originalLoad;
if (originalDatabaseUrl === undefined) delete process.env.DATABASE_URL;
else process.env.DATABASE_URL = originalDatabaseUrl;
if (originalRawDays === undefined) delete process.env.LOG_RAW_DAYS;
else process.env.LOG_RAW_DAYS = originalRawDays;

test('stores every MQTT payload as bytes with packet metadata', async () => {
  const firstPayload = Buffer.from([0x00, 0x7b, 0xff]);
  const secondPayload = Buffer.from('{"status":"offline"}', 'utf8');

  datalog.addMessage('plant/pump', firstPayload, 1760000000000, {
    qos: 1, retain: false, dup: true, messageId: 23,
  });
  datalog.addMessage('plant/pump', secondPayload, 1760000001000, {
    qos: 0, retain: true, dup: false,
  });
  await datalog.flushNow();

  const schemaQuery = queries.find(q => /create table if not exists mqtt_messages/i.test(q.text));
  const insertQuery = queries.find(q => /insert into mqtt_messages/i.test(q.text));
  assert.ok(schemaQuery);
  assert.ok(insertQuery);
  assert.equal(insertQuery.params.length, 14);
  assert.equal(insertQuery.params[0].getTime(), 1760000000000);
  assert.equal(insertQuery.params[1], 'plant/pump');
  assert.deepEqual(insertQuery.params[2], firstPayload);
  assert.equal(insertQuery.params[3], 1);
  assert.equal(insertQuery.params[4], false);
  assert.equal(insertQuery.params[5], true);
  assert.equal(insertQuery.params[6], 23);
  assert.deepEqual(insertQuery.params[9], secondPayload);
  assert.equal(insertQuery.params[10], 0);
  assert.equal(insertQuery.params[11], true);
  assert.equal(insertQuery.params[12], false);
  assert.equal(insertQuery.params[13], null);
  assert.equal(datalog.RAW_DAYS, 30);
  assert.equal(datalog.stat().rawPending, 0);
  assert.ok(pool);
});

test('reset all clears raw MQTT messages as well as numeric summaries', async () => {
  await datalog.reset(null);
  assert.ok(queries.some(q => /truncate table log_readings, log_hourly, mqtt_messages/i.test(q.text)));
});
