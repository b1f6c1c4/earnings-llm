require('dotenv').config();
const { MongoClient } = require('mongodb');
const { SimpleLinearRegression } = require('ml-regression-simple-linear');
const moment = require('moment-timezone');

const client = new MongoClient(process.env.MONGO_URL);

const fmt = (v) => {
  if (Math.abs(v) > 1e9)
    return `${(v / 1e9).toFixed(3)}B`;
  if (Math.abs(v) > 1e6)
    return `${(v / 1e6).toFixed(1)}M`;
  if (Math.abs(v) > 1e3)
    return `${(v / 1e3).toFixed(1)}k`;
  return `${v}`;
};

const compare = (a, e) => {
  if (a >= 1.01 * e) // both may be negative
    return 'beat';
  if (a <= 0.99 * e) // both may be negative
    return 'missed';
  return 'met';
};

const indexes = ['S & P 500', 'Russel 2000', 'Dow Jones', 'Nasdaq 100'];

const lgToPercent = (lg) => `${(Math.pow(10, 2 + lg) - 100).toFixed(3)}%`;

const analysis = (X, y) => {
  const reg = new SimpleLinearRegression(X, y);
  const orig = y.map(v => Math.pow(10, v));
  const range = Math.log10(Math.max(...orig) - Math.min(...orig));
  return {
    slope: reg.slope,
    range,
    min: Math.min(...y),
    max: Math.max(...y),
    ...reg.score(X, y),
  };
}

const describeEarnings = (coll) => async (doc) => {
  let s = '';
  s += `A public-traded company (symbol: ${doc._id.symbol}) reported their ${doc._id.quarter} quarterly earnings on ${moment(doc.date).format('dddd, MMMM Do YYYY')}, after the market closing bell.`;
  s += ` The company reported earnings of $${doc.epsActual.toFixed(2)} per share which `;
  s += compare(doc.epsActual, doc.epsEstimate);
  s += ` the analyst consensus estimate of $${doc.epsEstimate}`
  if (doc.epsActual >= 0 && doc.epsEstimate >= 0) {
    const ratio = 100 * (doc.epsActual / doc.epsEstimate - 1);
    if (ratio > 1)
      s += ` by ${ratio.toFixed(2)} percent.`;
    else if (ratio < -1)
      s += ` by ${(-ratio).toFixed(2)} percent.`;
    else
      s += '.';
  } else {
      s += '.';
  }
  s += ` The company reported quarterly sales of $${fmt(doc.revenueActual)} which `;
  s += compare(doc.revenueActual, doc.revenueEstimate);
  s += ` the analyst consensus estimate of $${fmt(doc.revenueEstimate)}`;
  if (doc.revenueActual >= 0 && doc.revenueEstimate >= 0) {
    const ratio = 100 * (doc.revenueActual / doc.revenueEstimate - 1);
    if (ratio > 1)
      s += ` by ${ratio.toFixed(2)} percent.`;
    else if (ratio < -1)
      s += ` by ${(-ratio).toFixed(2)} percent.`;
    else
      s += '.';
  } else {
      s += '.';
  }

  await coll.updateOne({ _id: doc._id }, {
    $set: { 'descriptions.earnings': s },
  });
};

const describePast = (coll) => async (doc) => {
  let s = '';
  s += `In the last ${doc.past.length} trading days, `;
  const X = doc.past.map((_,i) => i);
  const first = doc.past[0];
  const last = doc.past[doc.past.length - 1];
  let avgIndexSlope = 0;
  for (const ind of indexes) {
    s += `${ind} stock index `;
    const reg = analysis(X, doc.past.map(v => v.indexes[ind]));
    if (reg.slope >= Math.log10(1.007))
      s += 'showed a very strong bullish trend';
    else if (reg.slope > Math.log10(1.003))
      s += 'showed a strong bullish trend';
    else if (reg.slope > Math.log10(1.001))
      s += 'showed a moderate bullish trend';
    else if (reg.slope < -Math.log10(1.001))
      s += 'showed a moderate bearish trend';
    else if (reg.slope < -Math.log10(1.003))
      s += 'showed a strong bearish trend';
    else if (reg.slope < -Math.log10(1.007))
      s += 'showed a very strong bearish trend';
    else if (reg.range - reg.min > Math.log10(.03))
      s += 'was highly volatile and choppy';
    else if (reg.range - reg.min > Math.log10(.015))
      s += 'was moderately choppy';
    else if (reg.range - reg.min > Math.log10(.004))
      s += 'was slightly choppy';
    else
      s += 'remained stable';
    const rate = last.indexes[ind] - first.indexes[ind];
    if (rate > Math.log10(1.001))
      s += `, up by ${lgToPercent(rate)}. `;
    else if (rate < -Math.log10(1.001))
      s += `, down by ${lgToPercent(-rate)}. `;
    else
      s += '. ';
    avgIndexSlope += reg.slope;
  }
  avgIndexSlope /= indexes.length;

  {
    const reg = analysis(X, doc.past.map(v => v.lgMark));
    s += `This company's stock (symbol: ${doc._id.symbol}) has been `;
    if (reg.slope > Math.log10(1.05))
      s += 'surging';
    else if (reg.slope > Math.log10(1.01))
      s += 'trending bullish';
    else if (reg.slope > Math.log10(1.005))
      s += 'trending slightly bullish';
    else if (reg.slope < -Math.log10(1.005))
      s += 'trending slightly bearish';
    else if (reg.slope < -Math.log10(1.01))
      s += 'trending bearish';
    else if (reg.slope < -Math.log10(1.05))
      s += 'plunging';
    else if (reg.range - reg.min > Math.log10(.11))
      s += 'highly volatile and choppy';
    else if (reg.range - reg.min > Math.log10(.08))
      s += 'moderately choppy';
    else if (reg.range - reg.min > Math.log10(.05))
      s += 'slightly choppy';
    else
      s += 'moving sideways';
    const rate = last.lgMark - first.lgMark;
    if (rate > Math.log10(1.001))
      s += `, up by ${lgToPercent(rate)}`;
    else if (rate < -Math.log10(1.001))
      s += `, down by ${lgToPercent(-rate)}`;
    const rel = rate - avgIndexSlope;
    if (rel > Math.log10(1.08))
      s += ', overwhelming relative to the stock market.';
    if (rel > Math.log10(1.02))
      s += ', outperforming the stock market.';
    else if (rate < -Math.log10(1.02))
      s += ', underperforming the stock market.';
    else if (rate < -Math.log10(1.08))
      s += ', underwhelming relative to the stock market.';
    else
      s += ', moving in line with the stock market.';
  }
  s += '\n';

  await coll.updateOne({ _id: doc._id }, {
    $set: { 'descriptions.past': s },
  });
};

const describeEarningDay = (coll) => async (doc) => {
  let s = '';
// s += `On the trading day when after-bell earnings are scheduled, the company's stock`;

  await coll.updateOne({ _id: doc._id }, {
    $set: { 'descriptions.earningDay': s },
  });
};

const describeAfterMarket = (coll) => async (doc) => {
  let s = '';
// s += `On the trading day when after-bell earnings are scheduled, the company's stock`;

  await coll.updateOne({ _id: doc._id }, {
    $set: { 'descriptions.afterMarket': s },
  });
};

const describePreMarket = (coll) => async (doc) => {
  let s = '';
// s += `On the trading day when after-bell earnings are scheduled, the company's stock`;

  await coll.updateOne({ _id: doc._id }, {
    $set: { 'descriptions.preMarket': s },
  });
};

const describeNextDay = (coll) => async (doc) => {
  let s = '';
// s += `On the trading day when after-bell earnings are scheduled, the company's stock`;

  await coll.updateOne({ _id: doc._id }, {
    $set: { 'descriptions.nextDay': s },
  });
};

(async () => {
  await client.connect();
  const earnings_cleaned = client.db().collection('earnings_cleaned');
  const docs = await earnings_cleaned.find({
    $expr: {
      $and: [
        { $gt: [{ $size: '$afterMarket' }, 2] },
        { $gt: [{ $size: '$preMarket' }, 2] },
        { $gt: [{ $size: '$earningDay' }, 4] },
        { $gt: [{ $size: '$nextDay' }, 4] },
      ],
    },
  }).toArray();
  console.log(`working on ${docs.length} documents`);
  await Promise.all([
    Promise.all(docs.map(describeEarnings(earnings_cleaned))),
    Promise.all(docs.map(describePast(earnings_cleaned))),
    Promise.all(docs.map(describeEarningDay(earnings_cleaned))),
    Promise.all(docs.map(describeAfterMarket(earnings_cleaned))),
    Promise.all(docs.map(describePreMarket(earnings_cleaned))),
    Promise.all(docs.map(describeNextDay(earnings_cleaned))),
  ]);
  console.log('finishing');
  await client.close();
})();
