#!/usr/bin/env node

require('dotenv').config();
const fs = require('node:fs/promises');
const { default: pThrottle } = require('p-throttle');
const { MongoClient } = require('mongodb');
const { GoogleGenerativeAI } = require("@google/generative-ai");
const { OpenAI } = require('openai');
const { Ollama } = require('ollama');

const client = new MongoClient(process.env.MONGO_URL);
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const ollama = new Ollama({ host: process.env.OLLAMA_URL });
const groq = new OpenAI({
  apiKey: process.env.GROQ_API_KEY,
  baseURL: 'https://api.groq.com/openai/v1',
});

const invoker = (model, api) => async (fn) => {
  const coll = client.db().collection('llm_outputs');
  const m = fn.match(/^(?<symbol>.*)_(?<quarter>.*)\.txt$/);
  const _id = { symbol: m.groups.symbol, quarter: m.groups.quarter, model };
  if (await coll.findOne({ _id }))
    return;
  const prompt = await fs.readFile('desc/' + fn, 'utf8');
  const text = await api(prompt);
  console.log(`got answer from ${model} length ${text.length}`);
  const reg = /DO NOT TRADE .*|BUY .*|SELL .*/m;
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

const askGemini = (model) => {
  const llm = genAI.getGenerativeModel({ model })
  const apiFunc = async (x) => {
    console.log(`asking ${model} for ${x.length} long prompt, gemini`);
    return (await llm.generateContent(x)).response.text();
  };
  const throttle = pThrottle({
    limit: 1,
    interval: 4100,
  });
  return invoker(model, throttle(apiFunc));
};

const askGroq = (model) => {
  const apiFunc = async (x) => {
    console.log(`asking ${model} for ${x.length} long prompt, groq`);
    return (await groq.chat.completions.create({
      messages: [{ role: 'user', content: x }],
      model,
    })).choices[0].message.content;
  };
  const throttle = pThrottle({
    limit: 1,
    interval: 60000,
  });
  return invoker(model, throttle(apiFunc));
};

const askOllama = (model) => {
  const apiFunc = async (x) => {
    console.log(`asking ${model} for ${x.length} long prompt, ollama`);
    return (await ollama.chat({
      model,
      messages: [{ role: 'user', content: x }],
    })).message.content;
  };
  const throttle = pThrottle({
    limit: 1,
    interval: 2000,
  });
  return invoker(model, throttle(apiFunc));
};

(async () => {
  await client.connect();
  const dir = await fs.readdir('desc');
  console.log(`working on ${dir.length} entries`);
  await Promise.all(dir.map(askGemini('gemini-2.0-flash')));
  await Promise.all(dir.map(askGemini('gemini-1.5-flash')));
  await Promise.all(dir.map(askOllama('deepseek-r1:7b')));
  await Promise.all(dir.map(askOllama('llama-3.3:70b')));
  await Promise.all(dir.map(askGroq('llama-3.3-70b-versatile')));
  await Promise.all(dir.map(askGroq('deepseek-r1-distill-llama-70b')));
  await client.close();
})();
