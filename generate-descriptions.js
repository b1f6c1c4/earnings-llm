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

const percent = (r) => `${(100 * r).toFixed(2)}%`;

const lgToPercent = (lg) => `${(Math.pow(10, 2 + lg) - 100).toFixed(2)}%`;

const toTime = (tod) => {
  return moment.unix(tod * 60).utc().format('hh:mmA');
};

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
  s += `Yesterday, a public-traded company (symbol: ${doc._id.symbol}) reported their ${doc._id.quarter} quarterly earnings on a ${moment(doc.date).format('dddd')}, after the market closing bell.`;
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
  s += `Yesterday, which is the day when the after-bell earnings are scheduled, the company's stock `;
  const op = doc.earningDay[0].open;
  const cl = doc.earningDay[doc.earningDay.length - 1].close;
  {
    const rate = op / doc.past[doc.past.length - 1].close;
    if (rate > 1.01)
      s += `gapped up by ${percent(rate-1)}`;
    else if (rate < 0.99)
      s += `gapped down by ${percent(1/rate-1)}`;
    else
      s += `opened flat`;
    s += ` at ${op.toFixed(2)} at 09:30AM.\n`;
  }
  for (const v of doc.earningDay) {
    if (v.etTimeOfDay < 10 * 60 || v.etTimeOfDay > 15.5 * 60)
      continue;
    s += `At ${toTime(v.etTimeOfDay)}, the stock was trading at ${v.mark.toFixed(2)},`;
    const rate = v.lgMark - Math.log10(op);
    if (rate > Math.log10(1.001))
      s += ` up by ${lgToPercent(rate)}.\n`;
    else if (rate < -Math.log10(1.001))
      s += ` down by ${lgToPercent(-rate)}.\n`;
    else
      s += `.\n`;
  }
  {
    s += `The stock closed at ${cl.toFixed(2)} on the closing bell at 04:00PM,`;
    let rate = cl / doc.past[doc.past.length - 1].close;
    if (rate > 1.001)
      s += ` up by ${percent(rate-1)} relative to previous day's closing price`;
    else if (rate < 0.999)
      s += ` down by ${percent(1/rate-1)} relative to previous day's closing price`;
    else
      s += ` the same as previous day's closing price`;
    s += ' and';
    rate = cl / op;
    if (rate > 1.001)
      s += ` up by ${percent(rate-1)} relative to opening price.\n`;
    else if (rate < 0.999)
      s += ` down by ${percent(1/rate-1)} relative to opening price.\n`;
    else
      s += ` the same as opening price.\n`;
  }

  await coll.updateOne({ _id: doc._id }, {
    $set: { 'descriptions.earningDay': s },
  });
};

const describeAfterMarket = (coll) => async (doc) => {
  let s = '';
  const cl = doc.earningDay[doc.earningDay.length - 1].close;
  s += `After the closing bell, the company reported their earning data through an earning call. The market's reaction during yesterday's after-market EXT hours was: (all up/down values are relative to yesterday's closing price, ${cl.toFixed(2)})\n`;
  for (const v of doc.afterMarket) {
    s += `Around ${toTime(v.etTimeOfDay + 30)}, the stock was trading at ${v.mark.toFixed(2)},`;
    const rate = v.lgMark - Math.log10(cl);
    if (rate > Math.log10(1.001))
      s += ` up by ${lgToPercent(rate)}.\n`;
    else if (rate < -Math.log10(1.001))
      s += ` down by ${lgToPercent(-rate)}.\n`;
    else
      s += `.\n`;
  }

  await coll.updateOne({ _id: doc._id }, {
    $set: { 'descriptions.afterMarket': s },
  });
};

const describePreMarket = (coll) => async (doc) => {
  let s = '';
  const cl = doc.earningDay[doc.earningDay.length - 1].close;
  s += `After some EXTO trading activitities, today, the market's reaction during today's pre-market EXT hours was: (all up/down values are relative to yesterday's regular-hours closing price, ${cl.toFixed(2)})\n`;
  for (const v of doc.preMarket) {
    s += `Around ${toTime(v.etTimeOfDay + 15)}, the stock was trading at ${v.mark.toFixed(2)},`;
    const rate = v.lgMark - Math.log10(cl);
    if (rate > Math.log10(1.001))
      s += ` up by ${lgToPercent(rate)}.\n`;
    else if (rate < -Math.log10(1.001))
      s += ` down by ${lgToPercent(-rate)}.\n`;
    else
      s += `.\n`;
  }

  await coll.updateOne({ _id: doc._id }, {
    $set: { 'descriptions.preMarket': s },
  });
};

const askQuestion = (coll) => async (doc) => {
  let s = '';
  s += `It's 09:15AM now, and you have to predict today's market reaction to yesterday's after-bell earning report from ${doc._id.symbol}, starting from market opening at 09:30AM.
1. You must determine if you would buy long at 09:30AM, sell short at 09:30AM, or not trading this stock today. However, since you are a short-term day trader, you have to close your position by the end of today (excluding EXT hours), whether you profit or lose.
2. You must properly size your order, assume you can use at most 4x lever on a $50,000 capital. Your risk tolerance is moderate, and seeks short-term growth opportunities. You can express your position size either in number of stocks or a dollar amount.
3. You must determine both the price target (LMT) for ${doc._id.symbol} and a stop-loss price (STP). Either value could be expressed as a dollar amount (@) OR a signed percentage (%).

For example, your example output could be one of the following 5:
BUY +500 ${doc._id.symbol}; SELL LMT @90.00 STP -1%
SELL -200 ${doc._id.symbol}; BUY LMT -3% STP @89.46
SELL $3,000 of ${doc._id.symbol}; BUY LMT @30.00 STP -1%
BUY $60,000 of ${doc._id.symbol}; SELL LMT +2% STP @37.41
DO NOT TRADE ${doc._id.symbol}

Now, make the decision on ${doc._id.symbol}.
`;

  await coll.updateOne({ _id: doc._id }, {
    $set: { 'descriptions.question': s },
  });
};

function maxRisk(arr, lmt, dir) { // buy long = dir
  let left = 0, right = arr.length;
  while (left < right) {
    let mid = Math.floor((left + right) / 2);
    if (dir > 0 ? arr[mid].bidH >= lmt : arr[mid].askL <= lmt) {
      right = mid;
    } else {
      left = mid + 1;
    }
  }

  const obj = left < arr.length ? arr[left] : arr[arr.length - 1];
  return [dir > 0 ? obj.askL : obj.bidH, obj.etTimeOfDay];
}

const describeNextDay = (coll) => async (doc) => {
  let s = '';
  s += `After the 09:30AM opening bell, today's ${doc._id.symbol}`;
  {
    const first = doc.nextDay[0].open;
    const last = doc.nextDay[doc.nextDay.length - 1].close;
    const llast = doc.earningDay[doc.earningDay.length - 1].close;
    s += ` opened at ${first.toFixed(2)},`;
    let rate = first / llast;
    if (rate > 1.01)
      s += ` gapped up by ${percent(rate-1)} from yesterday (earnings day) closing price`;
    else if (rate < 0.99)
      s += ` gapped down by ${percent(1/rate-1)} from yesterday (earnings day) closing price`;
    else
      s += ` ${percent(rate-1)} from yesterday's closing price`;
    s += ` ${llast.toFixed(2)}.`;
    s += ` During the day, ${doc._id.symbol} `;
    const reg = analysis(doc.nextDay.map(v => v.etTimeOfDay), doc.nextDay.map(v => v.lgMark));
    if (reg.slope > Math.log10(1.10) / 12)
      s += 'absolutely roared';
    else if (reg.slope > Math.log10(1.05) / 12)
      s += 'surged';
    else if (reg.slope > Math.log10(1.01) / 12)
      s += 'trended bullish';
    else if (reg.slope > Math.log10(1.005) / 12)
      s += 'trended slightly bullish';
    else if (reg.slope < -Math.log10(1.005) / 12)
      s += 'trended slightly bearish';
    else if (reg.slope < -Math.log10(1.01) / 12)
      s += 'trended bearish';
    else if (reg.slope < -Math.log10(1.05) / 12)
      s += 'plunged';
    else if (reg.slope < -Math.log10(1.10) / 12)
      s += 'severely plunged';
    else if (reg.range - reg.min > Math.log10(.11))
      s += 'remained highly volatile and choppy';
    else if (reg.range - reg.min > Math.log10(.08))
      s += 'remained moderately choppy';
    else if (reg.range - reg.min > Math.log10(.05))
      s += 'remained slightly choppy';
    else
      s += 'moved sideways';
    rate = Math.log10(last) - Math.log10(first);
    s += `, closing at ${last.toFixed(2)}`;
    if (rate > Math.log10(1.001))
      s += `, up by ${lgToPercent(rate)} from today's opening price`;
    else if (rate < -Math.log10(1.001))
      s += `, down by ${lgToPercent(-rate)} from today's opening price`;
    else
      s += `, not moved much from today's opening price`;
    rate = Math.log10(last) - Math.log10(llast);
    if (rate > Math.log10(1.001))
      s += `, and up by ${lgToPercent(rate)} from today's closing price before earning call.`;
    else if (rate < -Math.log10(1.001))
      s += `, and down by ${lgToPercent(-rate)} from today's closing price before earning call.`;
    else
      s += `, and not moved much from yesterday's closing price before earning call.`;
  }
  s += '\n';
  const first = doc.nextDayBooks[0];
  const last = doc.nextDayBooks[doc.nextDayBooks.length - 1];
  const profitL = last.bidH / first.askL - 1;
  const profitS = 1 - last.askL / first.bidH;
  if (profitL > profitS && profitL > 0.001) {
    const tp = first.askL;
    const [risk, tod] = maxRisk(doc.nextDayBooks, tp, +1);
    s += ` If a trader entered a long position BUY at ${tp} around 09:30AM,`;
    s += ` set the price target to at most LMT @${last.bidH},`;
    s += ` set the stop loss at least STP @${risk},`;
    s += ` they could reap ${percent(profitL)} profit by ${toTime(tod)}`;
    if (risk > tp) s += ` as long as they can tolerate ${percent(risk/tp-1)} risk.`;
    else s += '.';
    const moc = doc.nextDayMOC[0].bid;
    s += ` Setting price target beyond ${last.bidH} would result in ${percent(moc/tp-1)}`;
    if (moc < tp)
      s += ' loss.\n';
    else
      s += ' profit.\n';
  } else if (profitS > profitL && profitS > 0.001) {
    const tp = first.bidH;
    const [risk, tod] = maxRisk(doc.nextDayBooks, tp, -1);
    s += ` If a trader entered short position SELL at ${tp} around 09:30AM,`;
    s += ` set the price target to at most LMT @${last.askL},`;
    s += ` set the stop loss at least STP @${risk},`;
    s += ` they could reap ${percent(profitS)} profit by ${toTime(tod)}`;
    if (risk > tp) s += ` as long as they can tolerate ${percent(risk/tp-1)} risk.`;
    else s += '.';
    const moc = doc.nextDayMOC[0].ask;
    s += ` Setting price target beyond ${last.askL} would result in ${percent(1-moc/tp)}`;
    if (moc > tp)
      s += ' loss.\n';
    else
      s += ' profit.\n';
  } else {
    s += `No trader could theoretically make profit at all from today's market because of the wide spread and lack of stock price movement.`;
  }

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
    Promise.all(docs.map(askQuestion(earnings_cleaned))),
  ]);
  console.log('finishing');
  await client.close();
})();
