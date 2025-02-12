require('dotenv').config();
const { MongoClient } = require('mongodb');
const fs = require('node:fs/promises');
const { SimpleLinearRegression } = require('ml-regression-simple-linear');

const client = new MongoClient(process.env.MONGO_URL);


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
    $project: {
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
  await fs.writeFile('descriptions.json', JSON.stringify(docs, null, 2));
  console.log('finishing');
  await client.close();
})();
