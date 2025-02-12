#!/usr/bin/env node

require('dotenv').config();
const fs = require('node:fs');
const finnhub = require('finnhub');
const { MongoClient } = require('mongodb');

const client = new MongoClient(process.env.MONGO_URL);

const download = () => new Promise((resolve, reject) => {
  const api_key = finnhub.ApiClient.instance.authentications['api_key'];
  api_key.apiKey = process.env.FINNHUB_API_KEY
  const finnhubClient = new finnhub.DefaultApi()
  finnhubClient.earningsCalendar({
    from: '2025-01-01',
    to: '2025-02-04',
  }, (error, data, response) => {
    if (error) reject(error);
    else resolve(data.earningsCalendar);
  });
});

(async () => {
  await client.connect();
  const earnings = client.db().collection('earnings');
  const data = await download();
  console.log(`${data.length} earnings entry downloaded`);
  await earnings.insertMany(data.map(d => ({
    _id: {
      symbol: d.symbol,
      quarter: `${d.year}Q${d.quarter}`,
    },
    date: new Date(d.date),
    hour: d.hour,
    epsEstimate: d.epsEstimate,
    epsActual: d.epsActual,
    revenueEstimate: d.revenueEstimate,
    revenueActual: d.revenueActual,
  })), {
    ordered: false,
  });
  console.log(`${data.length} earnings entry pushed to mongodb`);
})();
