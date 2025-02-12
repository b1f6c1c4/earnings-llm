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
  const docs = await earnings_cleaned.aggregate([{
    $match: {
      $expr: {
        $and: [
          { $gt: [{ $size: '$afterMarket' }, 2] },
          { $gt: [{ $size: '$preMarket' }, 2] },
          { $gt: [{ $size: '$earningDay' }, 4] },
          { $gt: [{ $size: '$nextDay' }, 4] },
        ],
      },
    },
  }, {
    $addFields: {
      asExample: { $concat: [
        '$descriptions.earnings', '\n',
        '$descriptions.past', '\n',
        '$descriptions.earningDay', '\n',
        '$descriptions.afterMarket', '\n',
        '$descriptions.preMarket', '\n',
        '$descriptions.nextDay',
      ] },
      asQuestion: { $concat: [
        '$descriptions.earnings', '\n',
        '$descriptions.past', '\n',
        '$descriptions.earningDay', '\n',
        '$descriptions.afterMarket', '\n',
        '$descriptions.preMarket', '\n',
        '$descriptions.question',
      ] },
    },
  }]).toArray();
  console.log(`working on ${docs.length} documents`);
  await fs.mkdir('desc', { recursive: true });
  await Promise.all(docs.map(doc => fs.writeFile(`desc/${doc._id.symbol}_${doc._id.quarter}.example.txt`, doc.asExample)));
  await Promise.all(docs.map(doc => fs.writeFile(`desc/${doc._id.symbol}_${doc._id.quarter}.question.txt`, doc.asQuestion)));
  await fs.writeFile('desc/full.json', JSON.stringify(docs, null, 2));
  console.log('finishing');
  await client.close();
})();
