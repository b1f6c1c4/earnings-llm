require('dotenv').config();
const { default: pThrottle } = require('p-throttle');
const { MongoClient } = require('mongodb');
const moment = require('moment-timezone');

const client = new MongoClient(process.env.MONGO_URL);

const apiKey = process.env.DATABENTO_API_KEY;
const headers = {
  'Authorization': `Basic ${Buffer.from(apiKey + ':').toString('base64')}`,
  'Content-Type': 'application/x-www-form-urlencoded'
};

const agg = [
  {
    '$match': {
      hour: 'amc',
      epsActual: { '$ne': null },
      epsEstimate: { '$ne': null },
      revenueActual: { '$ne': null },
      revenueEstimate: { '$ne': null },
    },
  }, {
    '$lookup': {
      from: 'symbol_ids',
      localField: '_id.symbol',
      foreignField: '_id',
      as: 's',
    }
  }, {
    '$unwind': {
      path: '$s',
      preserveNullAndEmptyArrays: false
    }
  }
]

const throttle = pThrottle({
	limit: 2,
	interval: 1000,
});

const api = throttle((body) => fetch('https://hist.databento.com/v0/timeseries.get_range', {
  method: 'POST',
  headers,
  body: new URLSearchParams(body),
}));

const loadPrice = (client, schema, func) => async (doc) => {
  const prices = client.db().collection('prices');
  const { _id: { symbol, quarter }, date, s } = doc;
  if (!s || !s.datasets.includes('XNAS.ITCH')) {
    console.dir(doc);
    return;
  }
  const found = await prices.findOne({ _id: { symbol, date }, [schema]: { $exists: true, $ne: [] } });
  if (found) {
    return;
  }
  const [start, end] = func(date);
  // console.log(`working on ${symbol} from ${start} to ${end} for ${schema}`);
  const body = {
    dataset: 'XNAS.ITCH',
    symbols: s.s,
    schema,
    start,
    end,
    encoding: 'json',
    // compression: 'zstd',
    stype_in: 'instrument_id',
    stype_out: 'instrument_id',
    pretty_px: true,
    pretty_ts: true,
  };
  const response = await api(body);
  if (!response.ok) {
    console.log(await response.text());
    return;
  }
  const data = await response.text();
  const entries = data ? data.trim().split('\n').map(s => JSON.parse(s)) : [];
  console.log(`writing ${entries.length} ${schema} results to ${symbol} ${quarter}`);
  await prices.updateOne({ _id: { symbol, date } }, {
    $set: { [schema]: entries },
  }, {
    upsert: true,
  });
};

function compute1Day(date) {
  const l = moment(date.toISOString().replace(/T.*/, 'T09:30:00'))
    .tz('America/New_York').toISOString();
  const r = moment(date.toISOString().replace(/T.*/, 'T16:00:00'))
    .tz('America/New_York').add(1, 'd').toISOString();
  return [l, r];
}

function computeDays(date) {
  const l = moment(date.toISOString().replace(/T.*/, ''))
    .tz('America/New_York').add(-10, 'd').toISOString();
  const r = moment(date.toISOString().replace(/T.*/, ''))
    .tz('America/New_York').add(-1, 'd').toISOString();
  return [l, r];
}

(async () => {
  await client.connect();
  const earnings = client.db().collection('earnings');
  const docs = await earnings.aggregate(agg).toArray();
  console.log(`working on ${docs.length} entries`);
  await Promise.all(docs.map(loadPrice(client, 'bbo-1m', compute1Day)));
  await Promise.all(docs.map(loadPrice(client, 'ohlcv-1m', compute1Day)));
  await Promise.all(docs.map(loadPrice(client, 'ohlcv-1h', compute1Day)));
  await Promise.all(docs.map(loadPrice(client, 'ohlcv-1d', computeDays)));
  await client.close();
})();
