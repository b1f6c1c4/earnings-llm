require('dotenv').config();
const { default: pThrottle } = require('p-throttle');
const { MongoClient } = require('mongodb');
const moment = require('moment-timezone');

const client = new MongoClient(process.env.MONGO_URL);

const throttle = pThrottle({
	limit: 20,
	interval: 100,
});

const agg = (client) => throttle(async (doc) => {
  const { _id: { symbol }, date } = doc;
  const prices_cleaned = client.db().collection('prices_cleaned');
  const t = (str, d = 0) => new Date(moment(date.toISOString().replace(/T.*/, 'T' + str))
    .tz('America/New_York').add(d, 'd').toISOString());
  console.log(`aggregating symbol ${symbol}`);
  const aggs = {
    past: [{
      $match: { 'meta.symbol': symbol, 'meta.interval': '1d' },
    }, {
      $lookup: {
        from: 'stock_indexes',
        let: { e: '$ts_event' },
        pipeline: [{
          $match: { $expr: { $eq: ['$ts_event', '$$e'] } },
        }, {
          $group: {
            _id: '$index',
            mark: { $first: '$mark' },
          },
        }, {
          $project: {
            _id: 0,
            k: '$_id',
            v: { $log10: '$mark' },
          },
        }],
        as: 'indexes',
      },
    }, {
      $project: { _id: 0, d: { $divide: [{ $subtract: [ '$ts_event', date ] }, 86400e3] }, mark: 1, lgMark: 1, volume: 1, high: 1, low: 1, indexes: { $arrayToObject: '$indexes' } },
    }],
    earningDay: [{
      $match: { ts_event: { $gte: t('09:30'), $lt: t('16:00') }, 'meta.symbol': symbol, 'meta.interval': '1m', schema: 'ohlcv-1m' },
    }, {
      $bucket: {
        groupBy: '$etTimeOfDay',
        boundaries: [9.5, 10.0, 10.5, 11.0, 11.5, 12.0, 12.5, 13.0, 13.5, 14.0, 14.5, 15.0, 15.5, 16.0].map(x => x*60),
        output: { count: { $count: {} }, high: { $max: '$high' }, low: { $min: '$low' }, volume: { $sum: '$volume' }, mark: { $avg: '$mark' } },
      },
    }, {
      $project: { _id: 0, count: 1, etTimeOfDay: '$_id', mark: 1, lgMark: { $log10: '$mark' }, volume: 1, high: 1, low: 1 },
    }],
    afterMarket: [{
      $match: { ts_event: { $gte: t('16:00'), $lt: t('19:00') }, 'meta.symbol': symbol, 'meta.interval': '1m' },
    }, {
      $bucket: {
        groupBy: '$etTimeOfDay',
        boundaries: [16.0, 16.5, 17.0, 17.5, 18.0, 18.5, 19.0].map(x => x*60),
        output: { count: { $count: {} }, mark: { $avg: '$mark' } },
      },
    }, {
      $project: { _id: 0, count: 1, etTimeOfDay: '$_id', mark: 1, lgMark: { $log10: '$mark' }, volume: 1, high: 1, low: 1 },
    }],
    preMarket: [{
      $match: { ts_event: { $gte: t('04:00', 1), $lt: t('09:15', 1) }, 'meta.symbol': symbol, 'meta.interval': '1m' },
    }, {
      $bucket: {
        groupBy: '$etTimeOfDay',
        boundaries: [4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 7.5, 8.0, 8.5, 9.0, 9.25].map(x => x*60),
        output: { count: { $count: {} }, mark: { $avg: '$mark' } },
      },
    }, {
      $project: { _id: 0, count: 1, etTimeOfDay: '$_id', mark: 1, lgMark: { $log10: '$mark' }, volume: 1, high: 1, low: 1 },
    }],
    nextDay: [{
      $match: { ts_event: { $gte: t('09:30', 1), $lt: t('16:00', 1) }, 'meta.symbol': symbol, 'meta.interval': '1m', schema: 'ohlcv-1m' },
    }, {
      $bucket: {
        groupBy: '$etTimeOfDay',
        boundaries: [9.5, 10.0, 10.5, 11.0, 11.5, 12.0, 12.5, 13.0, 13.5, 14.0, 14.5, 15.0, 15.5, 16.0].map(x => x*60),
        output: { count: { $count: {} }, high: { $max: '$high' }, low: { $min: '$low' }, volume: { $sum: '$volume' }, mark: { $avg: '$mark' } },
      },
    }, {
      $project: { _id: 0, count: 1, etTimeOfDay: '$_id', mark: 1, lgMark: { $log10: '$mark' }, volume: 1, high: 1, low: 1 },
    }],
    nextDayBooks: [{
      $match: { ts_event: { $gte: t('09:30', 1), $lte: t('19:00', 1) }, 'meta.symbol': symbol, 'meta.interval': '1m', schema: 'bbo-1m' },
    }, {
      $setWindowFields: {
        sortBy: { ts_event: 1 },
        output: {
          bidH: { $max: '$bid', window: { documents: ['unbounded', 'current'] } },
          askL: { $min: '$ask', window: { documents: ['unbounded', 'current'] } },
        },
      },
    }, {
      $group: { _id: { bidH: '$bidH', askL: '$askL' }, etTimeOfDay: { $min: '$etTimeOfDay' } },
    }, {
      $project: { _id: 0, etTimeOfDay: 1, bidH: '$_id.bidH', askL: '$_id.askL' },
    }, {
      $sort: { etTimeOfDay: 1 },
    }],
  };
  const res = Object.fromEntries(await Promise.all(Object.entries(aggs).map(async ([k, v]) =>
    [k, await prices_cleaned.aggregate(v).toArray()])));
  if (!res.past.length) {
    console.log(`no data for symbol ${symbol}`);
    return;
  }
  const earnings_cleaned = client.db().collection('earnings_cleaned');
  await earnings_cleaned.replaceOne({ _id: doc._id }, {
    ...doc, ...res,
  }, { upsert: true });
});

(async () => {
  await client.connect();
  const earnings = client.db().collection('earnings');
  const docs = await earnings.find({
    hour: 'amc',
    epsActual: { '$ne': null },
    epsEstimate: { '$ne': null },
    revenueActual: { '$ne': null },
    revenueEstimate: { '$ne': null },
  }).toArray();
  await Promise.all(docs.map(agg(client)));
  console.log('finishing');
  await client.close();
})();
