require('dotenv').config();
const { MongoClient } = require('mongodb');

const client = new MongoClient(process.env.MONGO_URL);

const apiKey = process.env.DATABENTO_API_KEY;
const headers = {
  'Authorization': `Basic ${Buffer.from(apiKey + ':').toString('base64')}`,
  'Content-Type': 'application/x-www-form-urlencoded'
};

const loadSymbols = (client) => async (dataset) => {
  const response = await fetch('https://hist.databento.com/v0/symbology.resolve', {
    method: 'POST',
    headers,
    body: new URLSearchParams({
      dataset,
      symbols: 'ALL_SYMBOLS',
      stype_in: 'raw_symbol',
      stype_out: 'instrument_id',
      start_date: '2025-02-01',
      end_date: '2025-02-02',
    }),
  });
  const data = await response.json();
  const symbols = client.db().collection('symbols');
  if (!data.result) {
    console.log(dataset + ':');
    console.log(data);
    return;
  }
  await symbols.insertMany(Object.entries(data.result).map(([k,v]) => ({
    _id: {
      symbol: k,
      dataset,
    },
    s: v.length ? v[0].s : unll,
  })), {
    ordered: false,
  });
  console.log(`${Object.entries(data.result).length} symbols from ${dataset} entry pushed to mongodb`);
};

(async () => {
  await client.connect();
  const response = await fetch('https://hist.databento.com/v0/metadata.list_datasets', {
    method: 'GET',
    headers,
  });
  const data = await response.json();
  await Promise.all(data.map(loadSymbols(client)));
  await client.close();
})();
