require('dotenv').config();
const { MongoClient } = require('mongodb');
const fs = require('node:fs/promises');
const { SimpleLinearRegression } = require('ml-regression-simple-linear');

const client = new MongoClient(process.env.MONGO_URL);

const goodExamples = [
  { symbol: 'TER', quarter: '2024Q4' }, // BUY +$200,000 of TER; SELL LMT +11.30% STP -0.24%
  { symbol: 'TRNS', quarter: '2025Q3' }, // SELL -$186,800 of TRNS; BUY LMT -10.84% STP +0.27%
  { symbol: 'CMPR', quarter: '2025Q2' }, // DO NOT TRADE CMPR
];

(async () => {
  await client.connect();
  const earnings_cleaned = client.db().collection('earnings_cleaned');
  const examples = await earnings_cleaned.aggregate([{
    $match: {
      _id: { $in: goodExamples },
    },
  }, {
    $project: {
      _id: 0,
      text: { $concat: [
        '# Example of ', '$_id.symbol', `'s `, '$_id.quarter', ' Earnings Report\n\n',
        '$descriptions.earnings', '\n',
        '$descriptions.past', '\n',
        '$descriptions.earningDay', '\n',
        '$descriptions.afterMarket', '\n',
        '$descriptions.preMarket', '\n',
        '$descriptions.nextDay', '\n',
      ] },
    },
  }]).toArray();
  const docs = await earnings_cleaned.find({
    $expr: {
      $and: [
        { $not: { $in: ['$_id', goodExamples] } },
        { $gt: [{ $size: '$afterMarket' }, 2] },
        { $gt: [{ $size: '$preMarket' }, 2] },
        { $gt: [{ $size: '$earningDay' }, 4] },
        { $gt: [{ $size: '$nextDay' }, 4] },
      ],
    },
  }).toArray();
  console.log(`working on ${docs.length} documents`);
  await fs.mkdir('desc', { recursive: true });
  await Promise.all(docs.map(doc => fs.writeFile(`desc/${doc._id.symbol}_${doc._id.quarter}.txt`,
    `# Context
You are a short-term day trader on 4x lever with $50,000 capital. It's 09:15AM now, and you have to predict today's market reaction to yesterday's after-bell earnings report from ${doc._id.symbol}, starting from market opening at 09:30AM.

# Task
1. You must determine if you would buy long at 09:30AM, sell short at 09:30AM, or not trading ${doc._id.symbol} today. Be aware that you have to close all of your position by the end of today (excluding EXT hours), whether you profit or lose.
2. You must properly size your order, risk at most 1% of your capital. You must express your position in a dollar amount.
3. You must determine both the price target (LMT) for ${doc._id.symbol} and a stop-loss price (STP). Either value could be expressed as a dollar amount (@) OR a signed percentage (%).
4. You must conclude your reasoning with a single line of trade order.

# Example of Some Valid Trade Orders

BUY +$27,400 ${doc._id.symbol}; SELL LMT @90.00 STP -1%
SELL -$82,300 ${doc._id.symbol}; BUY LMT -3% STP @89.46
SELL -$3,000 of ${doc._id.symbol}; BUY LMT @30.00 STP -1%
BUY +$60,000 of ${doc._id.symbol}; SELL LMT +2% STP @37.41
DO NOT TRADE ${doc._id.symbol}

${examples.map(d => d.text).join('\n')}

# Input for You to Process: ${doc._id.symbol}'s ${doc._id.quarter} Earnings Report

${doc.descriptions.earnings}
${doc.descriptions.past}
${doc.descriptions.earningDay}
${doc.descriptions.afterMarket}
${doc.descriptions.preMarket}

Now, make the decision on ${doc._id.symbol}, generate a trade order.
`)));
  console.log('finishing');
  await client.close();
})();
