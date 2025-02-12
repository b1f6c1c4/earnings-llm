require('dotenv').config();
const { MongoClient } = require('mongodb');

const client = new MongoClient(process.env.MONGO_URL);

const cleanOHLCV = (field) => ({
  open: { $toDouble: `$${field}.open` },
  high: { $toDouble: `$${field}.high` },
  low: { $toDouble: `$${field}.low` },
  close: { $toDouble: `$${field}.close` },
  volume: { $toDouble: `$${field}.volume` },
});

const markOHLCV = {
  $divide: [{
    $add: ['$open', '$close', '$open', '$close', '$high', '$low'],
  }, 6 ],
};

const cleanBBO = (field) => ({
  ts_recv: { $toDate: `$${field}.ts_recv` },
  last: { $toDouble: `$${field}.price` },
  bid: { $toDouble: { $getField: { field: 'bid_px', input: { $first: `$${field}.levels` } } } },
  ask: { $toDouble: { $getField: { field: 'ask_px', input: { $first: `$${field}.levels` } } } },
});

const markBBO = {
  $divide: [{
    $add: ['$bid', '$ask', '$last', '$bid', '$ask'],
  }, 5 ],
};

const agg = (field, proj, mark) => [{
  $unwind: '$' + field,
}, {
  $project: {
    _id: 0,
    ts_event: { $toDate: `$${field}.hd.ts_event` },
    meta: {
      symbol: '$_id.symbol',
      interval: field.replace(/^.*-/, ''),
    },
    schema: field,
    ...proj(field),
  },
}, {
  $addFields: {
    mark,
    et: {
      $dateToParts: {
        date: '$ts_event',
        timezone: 'America/New_York',
      },
    },
  },
}, {
  $match: { mark: { $ne: null } },
}, {
  $addFields: {
    lgMark: { $log10: '$mark' },
    etDate: {
      $dateFromParts: { year: '$et.year', month: '$et.month', day: '$et.day' },
    },
    etTimeOfDay: {
      $add: [
        { $multiply: ['$et.hour', 60] },
        '$et.minute',
        { $divide: ['$et.second', 60] },
        { $divide: ['$et.millisecond', 60e3] },
      ],
    },
  },
}, {
  $merge: { into: 'prices_cleaned_tmp' },
}];

(async () => {
  await client.connect();
  const prices = client.db().collection('prices');
  console.log('dropping collection prices_cleaned');
  await client.db().dropCollection('prices_cleaned');
  console.log('dropping collection prices_cleaned_tmp');
  await client.db().dropCollection('prices_cleaned_tmp');
  console.log('issuing aggregates to prices_cleaned_tmp');
  await Promise.all([
    prices.aggregate(agg('bbo-1m',   cleanBBO,   markBBO)).toArray(),
    prices.aggregate(agg('ohlcv-1m', cleanOHLCV, markOHLCV)).toArray(),
    prices.aggregate(agg('ohlcv-1h', cleanOHLCV, markOHLCV)).toArray(),
    prices.aggregate(agg('ohlcv-1d', cleanOHLCV, markOHLCV)).toArray(),
  ]);
  console.log('copying from prices_cleaned_tmp to prices_cleaned');
  await client.db().collection('prices_cleaned_tmp').aggregate([{
    $match: {
      ts_event: { $ne: null },
    },
  }, {
    $out: {
      db: 'earnings',
      coll: 'prices_cleaned',
      timeseries: {
        timeField: 'ts_event',
        metaField: 'meta',
        granularity: 'minutes',
      },
    },
  }]).toArray();
  console.log('dropping collection prices_cleaned_tmp');
  await client.db().dropCollection('prices_cleaned_tmp');
  console.log('finishing');
  await client.close();
})();
