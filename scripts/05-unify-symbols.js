#!/usr/bin/env node

require('dotenv').config();
const { MongoClient } = require('mongodb');

const client = new MongoClient(process.env.MONGO_URL);

const agg = [
  {
    $group: {
      _id: '$_id.symbol',
      ss: { $push: '$s' },
      datasets: { $push: '$_id.dataset' },
    },
  }, {
    $project: {
      s: { $first: { $setUnion: [ '$ss' ] } },
      datasets: '$datasets',
      unique: { $eq: [ { $size: { $setUnion: [ '$ss' ] } }, 1 ] },
    },
  }, {
    $out: 'symbol_ids',
  },
];

(async () => {
  await client.connect();
  const symbols = client.db().collection('symbols');
  console.log(await symbols.aggregate(agg).toArray());
  await client.close();
})();
