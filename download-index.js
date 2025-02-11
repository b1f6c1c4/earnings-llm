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

const loadPrice = (client) => async ([symbol, index]) => {
  const body = {
    dataset: 'XNAS.ITCH',
    symbols: symbol,
    schema: 'ohlcv-1d',
    start: '2025-01-01',
    end: '2025-02-08',
    encoding: 'json',
    stype_in: 'raw_symbol',
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
  if (!data) {
    console.log(`No data to ${index}: ${symbol}`);
    return;
  }
  const entries = data ? data.trim().split('\n').map(s => JSON.parse(s)) : [];
  console.log(`writing ${entries.length} results to ${index}: ${symbol}`);
  const stock_indexes = client.db().collection('stock_indexes');
  await stock_indexes.insertMany(entries.map(doc => ({
    ts_event: new Date(doc.hd.ts_event),
    index,
    symbol,
    high: +doc.high,
    low: +doc.low,
    mark: (2 * doc.open + 2 * doc.close + 1 * doc.high + 1 * doc.low) / 6,
    lgMark: Math.log10((2 * doc.open + 2 * doc.close + 1 * doc.high + 1 * doc.low) / 6),
  })), {
    ordered: false,
  });
};

(async () => {
  await client.connect();
  console.log('dropping collection stock_indexes');
  await client.db().dropCollection('stock_indexes');
  console.log('creating collection stock_indexes');
  await client.db().createCollection('stock_indexes', {
    timeseries: {
      timeField: 'ts_event',
      metaField: 'symbol',
      granularity: 'hours',
    },
  });
  await Promise.all(Object.entries({
    SPY: 'S & P 500',
    DIA: 'Dow Jones',
    IWM: 'Russel 2000',
    QQQ: 'Nasdaq 100',
  })
    .map(loadPrice(client)));
  await client.close();
})();
