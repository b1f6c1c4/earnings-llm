#!/usr/bin/env node

require('dotenv').config();
const { MongoClient } = require('mongodb');

const parse = async (client, res) => {
  const p = {};
  const o = { entry: p };

  const doc = await client.db().collection('earnings_cleaned').findOne({
    '_id.symbol': res._id.symbol,
    '_id.quarter': res._id.quarter,
  });
  o.optimal = doc.optimal;
  if (doc.optimal.order.startsWith('BUY'))
    o.optimal.side = 'BUY';
  else if (doc.optimal.order.startsWith('SELL'))
    o.optimal.side = 'SELL';
  else
    o.optimal.side = 'NEITHER';
  o.date = doc.date;

  const reg = /(?<side>BUY|SELL)\s+(?<position>[+-]\$?[0-9,]+\.?[0-9]*)\s+(?:of\s+)?(?<symbol>[A-Z]+);\s*(?:BUY|SELL)?\s+LMT\s+@?(?<limit>\$?[0-9]+\.?[0-9]*|[+-][0-9]+\.?[0-9]*%)\s+STP\s+@?(?<stop>\$?[0-9]+\.?[0-9]*|[+-][0-9]+\.?[0-9]*%)|DO NOT TRADE (?<symbol>[A-Z]+)/;
  if (res.text) {
    const matches = [...res.text.matchAll(new RegExp(reg, 'mg'))];
    if (matches.length === 0) {
      o.error = 'no valid order detected';
      return o;
    }
    if (matches.length > 1) {
      o.orders = matches.map(m => m[0]);
    }
    res.order = matches[matches.length - 1][0];
  }
  if (!res.order) {
    o.error = 'no order detected';
    return o;
  }
  const match = res.order.match(reg);
  if (!match) {
    o.error = 'order syntax error';
    return o;
  }
  if (match.groups.symbol !== res._id.symbol) {
    o.error = 'symbol not matching';
    return o;
  }
  if (match.groups.side === undefined) {
    p.side = 'NEITHER';
    p.position = 0;
    o.profit = 0;
    o.return = 0;
    o.error = null;
    return o;
  }
  if (match.groups.position.startsWith('$')) {
    o.error = 'position not specified in dollar amount';
    return o;
  }
  p.position = +match.groups.position.replaceAll(/[\$,]/g, '');
  if (!p.position) {
    o.error = 'invalid position';
    return o;
  }

  p.price = match.groups.side === 'BUY'
    ? doc.nextDayBooks[0].askL
    : doc.nextDayBooks[0].bidH;
  if (match.groups.side === 'BUY') {
    if (p.position <= 0) {
      o.error = 'position sign error';
      return o;
    }
    p.side = 'BUY';
    p.shares = Math.floor(p.position / p.price);
    if (!p.shares) {
      o.error = 'buying less than 1 share';
      return o;
    }
  } else if (match.groups.side === 'SELL') {
    if (p.position >= 0) {
      o.error = 'position sign error';
      return o;
    }
    p.side = 'SELL';
    p.shares = -Math.floor(-p.position / p.price);
    if (!p.shares) {
      o.error = 'selling less than 1 share';
      return o;
    }
  } else {
    // unreachable
  }

  p.position = p.shares * p.price;
  if (match.groups.limit.endsWith('%')) {
    p.limit = +match.groups.limit.substr(0, match.groups.limit.length - 1);
    if (p.shares > 0)
      p.limit = p.price * (1 + p.limit / 100);
    else
      p.limit = p.price * (1 + p.limit / 100);
  } else {
    p.limit = +match.groups.limit;
  }
  if (isNaN(p.limit)) {
    o.error = 'syntax error';
    return o;
  }
  if (p.shares > 0 && p.limit < p.price) {
    o.error = 'sell limit too low';
    return o;
  }
  if (p.shares < 0 && p.limit > p.price) {
    o.error = 'buy-to-cover limit too high';
    return o;
  }
  if (match.groups.stop.endsWith('%')) {
    p.stop = +match.groups.stop.substr(0, match.groups.stop.length - 1);
    if (p.shares > 0)
      p.stop = p.price * (1 + p.stop / 100);
    else
      p.stop = p.price * (1 + p.stop / 100);
  } else {
    p.stop = +match.groups.stop;
  }
  if (isNaN(p.stop)) {
    o.error = 'syntax error';
    return o;
  }
  if (p.shares > 0 && p.stop > p.price) {
    o.error = 'sell stop too high';
    return o;
  }
  if (p.shares < 0 && p.stop < p.price) {
    o.error = 'buy-to-cover stop too low';
    return o;
  }

  for (let i = 1; i < doc.nextDayBooks.length; i++) {
    const v = doc.nextDayBooks[i];
    if (p.shares > 0 && v.bidH >= p.limit) {
      o.exit = { time: v.etTimeOfDay, price: v.bidH, type: 'SOLD LMT' };
      break;
    }
    if (p.shares < 0 && v.askL <= p.limit) {
      o.exit = { time: v.etTimeOfDay, price: v.askL, type: 'BOUGHT LMT' };
      break;
    }
    if (p.shares > 0 && v.askL < p.stop || p.shares < 0 && v.bidH > p.stop) {
      const info = {
        schema: 'bbo-1m',
        'meta.symbol': res._id.symbol,
        'meta.interval': '1m',
        etDate: new Date(+doc.date + 86400000),
        etTimeOfDay: v.etTimeOfDay,
      };
      const book = await client.db().collection('prices_cleaned').findOne(info);
      if (!book) {
        o.error = 'cannot find book info';
        p.info = info;
        return o;
      }
      if (p.shares > 0)
        o.exit = { time: v.etTimeOfDay, price: book.bid, type: 'SOLD STP' };
      else
        o.exit = { time: v.etTimeOfDay, price: book.ask, type: 'BOUGHT STP' };
      break;
    }
  }
  if (!o.exit) {
    if (p.shares > 0)
      o.exit = { time: 16 * 60, price: doc.nextDayMOC[0].bid, type: 'SOLD MOC' };
    else
      o.exit = { time: 16 * 60, price: doc.nextDayMOC[0].ask, type: 'BOUGHT MOC' };
  }

  o.profit = p.shares * (o.exit.price - p.price);
  if (p.shares > 0)
    o.return = o.exit.price / p.price - 1;
  else
    o.return = 1 - o.exit.price / p.price;
  o.error = null;
  return o;
};


const client = new MongoClient(process.env.MONGO_URL);

(async () => {
  await client.connect();
  const coll = client.db().collection('llm_outputs');
  const res = await coll.find({}).toArray();
  console.log(`working on ${res.length} documents`);
  await Promise.all(res.map(async (r) => coll.updateOne({ _id: r._id }, {
    $set: await parse(client, r),
  })));
  console.log('finishing');
  await client.close();
})();
