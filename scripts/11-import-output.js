#!/usr/bin/env node

require('dotenv').config();
const { MongoClient } = require('mongodb');
const fs = require('node:fs/promises');

const client = new MongoClient(process.env.MONGO_URL);

const writeDb = (client) => (line) => {
  const fields = line.split('\t');
  if (fields.length !== 2 + process.argv.length - 3) {
    console.log(`format wrong: ${line}`);
    return [];
  }
  const res = [];
  for (let i = 0; i < process.argv.length - 3; i++) {
    const model = process.argv[i + 3];
    if (!model)
      continue;
    if (!fields[2 + i])
      continue;
    if (!fields[2 + i].match(/^BUY|^SELL|^DO NOT TRADE/))
      continue;
    res.push({
      _id: {
        symbol: fields[0],
        quarter: fields[1],
        model,
      },
      order: fields[2 + i],
    });
  }
  return res;
};

(async () => {
  await client.connect();
  const file = await fs.readFile(process.argv[2], 'utf8');
  const lines = file.split('\n');
  console.log(`working on ${lines.length} documents`);
  const coll = client.db().collection('llm_outputs');
  console.log(lines.flatMap(writeDb(client)));
  // await coll.insertMany(lines.flatMap(writeDb(client)), {
  //   ordered: false,
  // });
  console.log('finishing');
  await client.close();
})();
