#!/usr/bin/env node

require('dotenv').config();
const fs = require('node:fs/promises');
const { default: pThrottle } = require('p-throttle');
const { MongoClient } = require('mongodb');
const { GoogleGenerativeAI } = require("@google/generative-ai");

const client = new MongoClient(process.env.MONGO_URL);
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

const throttle = pThrottle({
  limit: 1,
  interval: 4100,
});
const askGemini = (model) => {
  const llm = genAI.getGenerativeModel({ model });
  const coll = client.db().collection('llm_outputs');
  const reg = /^(DO NOT TRADE .*|BUY .*|SELL .*)/m;
  const api = throttle(async (x) => {
    console.log(`asking ${model} for ${x.length} long prompt`);
    return llm.generateContent(x);
  });
  return async (fn) => {
    const m = fn.match(/^(?<symbol>.*)_(?<quarter>.*)\.txt$/);
    const _id = { symbol: m.groups.symbol, quarter: m.groups.quarter, model };
    if (await coll.findOne({ _id }))
      return;
    const prompt = await fs.readFile('desc/' + fn, 'utf8');
    const result = await api(prompt);
    const text = result.response.text();
    console.log(`got answer from ${model} length ${text.length}`);
    const mm = text.match(reg);
    await coll.updateOne({ _id }, {
      $set: {
        text,
        order: mm ? mm[0] : null,
      },
    }, {
      upsert: true,
    });
  };
};

(async () => {
  await client.connect();
  const dir = await fs.readdir('desc');
  console.log(`working on ${dir.length} entries`);
  await Promise.all(dir.map(askGemini('gemini-2.0-flash')));
  await Promise.all(dir.map(askGemini('gemini-1.5-flash')));
  await client.close();
})();
