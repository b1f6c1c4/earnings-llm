#!/usr/bin/env node

require('dotenv').config();
const { MongoClient } = require('mongodb');
const fs = require('node:fs/promises');

const client = new MongoClient(process.env.MONGO_URL);


(async () => {
  await client.connect();
  const coll = client.db().collection('llm_outputs');
  const result = await coll.aggregate([{
    $match: {
      error: null,
    },
  }]).toArray();
  result.forEach(d => { delete d.text; });
  await fs.mkdir('visual', { recursive: true });
  await fs.writeFile('visual/data.json', JSON.stringify(result));
  await fs.writeFile('visual/data.csv', 'symbol,quarter,model,profit,return,position,llm_action,profitible_action,exit\r\n' + result.map((doc) =>
    `${doc._id.symbol},${doc._id.quarter},${doc._id.model},${doc.profit},${doc.return},${doc.entry.position},${doc.entry.side},${doc.optimal.side},${doc.exit ? doc.exit.type : 'N/A'}\r\n`
  ).join(''));
  await client.close();
})();
