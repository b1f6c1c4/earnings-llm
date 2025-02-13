#!/usr/bin/env node

require('dotenv').config();
const { MongoClient } = require('mongodb');
const fs = require('node:fs/promises');

const client = new MongoClient(process.env.MONGO_URL);

const getR = (v) => 1.0 * Math.pow(Math.abs(v), 0.3);

const svg = (idx) => (doc, index) => {
  let id;
  for (id = 0; id < idx.length; id++)
    if (idx[id]._id.symbol === doc._id.symbol
      && idx[id]._id.quarter === doc._id.quarter)
      break;
  const x = 8 * id;
  const s = doc._id.symbol;
  if (doc.entry.side === 'NEITHER') {
    return `
      <text x="${x-s.length*5/2*0.60}" y="-1.5" font-size="5" fill="black" transform="rotate(90,${x},0)">${s}</text>
`;
  }
  let y = -4e2 * doc.return;
  if (y < -120)
    y= -120;
  if (y > +120)
    y= +120;
  const radius = getR(doc.profit);
  const color = doc.profit < 0 ? '#ff0000' : '#00ff00';
  const fs = Math.min(Math.max(radius, 6), 10);
  return `
    <text x="${x-s.length*fs/2*0.60}" y="${y+fs*0.3}" font-size="${fs}" fill="black">${s}</text>
    <circle cx="${x}" cy="${y}" r="${radius}" fill="${color}55" />
    <line x1="${x}" y1="${y}" x2="${x}" y2="0" stroke="${color}22" />
`;
}

function headline(symbol, docs) {
  let num;
  let cls;
  const t = docs.reduce((s, d) => d.entry.side !== 'NEITHER' ? s + 1 : s, 0);
  const profit = docs.reduce((s, d) => s + d.profit, 0) / docs.length;
  if (profit > 0) {
    cls = 'profit';
    num = `+$${Math.round(profit).toLocaleString(0)}  +${(100*profit/5e4).toFixed(2)}%`;
  } else if (profit < 0) {
    cls = 'loss';
    num = `-$${Math.round(-profit).toLocaleString(0)}  -${(-100*profit/5e4).toFixed(2)}%`;
  } else {
    cls = 'neutral';
    num = '±$0  ±0.00%';
  }
  return `<div class="headline">
  <h1>${symbol}</h1>
  <h2>avg. <span class="${cls}">${num}</span></h2>
  <h3>${docs.length} decisions&nbsp;&nbsp;${t} trades</h3>
</div>`;
}

(async () => {
  await client.connect();
  const coll = client.db().collection('llm_outputs');
  const idx = await coll.aggregate([{
    $group: {
      _id: { symbol: '$_id.symbol', quarter: '$_id.quarter' },
      date: { $first: '$date' },
    },
  }, {
    $sort: {
      date: 1,
      '_id.symbol': 1,
      '_id.quarter': 1,
    },
  }, {
    $project: {
      _id: 1,
    },
  }]).toArray();
  const len = idx.length;
  const res = await coll.aggregate([{
    $match: {
      order: { $ne: null },
      error: null,
    },
  }, {
    $group: {
      _id: '$_id.model',
      docs: { $push: '$$ROOT' },
    },
  }, {
    $sort: {
      _id: 1,
    },
  }]).toArray();
  const html = `
<!DOCTYPE html>
<html>
  <head>
    <title>Timeline Visualization</title>
    <style>
      * { margin: 0; padding: 0; }
      div { position: relative; width: 100%; }
      .headline { position: absolute; width: 100%; }
      @media print {
        .headline { display: flex; justify-content: space-evenly; }
      }
      h1 { text-align: center; font-size: 23px; }
      h2 { text-align: center; font-size: 20px; }
      h3 { text-align: center; font-size: 18; }
      .profit { color: #0f0e; }
      .loss { color: #f00e; }
      .neutral { color: #777e; }
    </style>
  </head>
  <body>
    ${res.map(({ _id, docs }) => `
    <div>
      ${headline(_id, docs)}
      <svg width="100%" viewBox="-30 -90 ${34+8*len} 210">
        <line x1="-17" y1="-40" y2="+40" x2="-17" stroke="#333e" />
        <line x1="-23" y1="-40" y2="-40" x2="-11" stroke="#333e" />
        <line x1="-23" y1="+40" y2="+40" x2="-11" stroke="#333e" />
        <text x="-26" y="-45" font-size="12" fill="black">+1%</text>
        <text x="-26" y="+54" font-size="12" fill="black">-1%</text>
        <circle cx="400" cy="+80" r="${getR(1000)}" fill="#333e" />
        <text x="404" y="+84" font-size="12" fill="black">=$1,000</text>
        <circle cx="500" cy="+80" r="${getR(3000)}" fill="#333e" />
        <text x="508" y="+84" font-size="12" fill="black">=$3,000</text>
        <circle cx="600" cy="+80" r="${getR(10000)}" fill="#333e" />
        <text x="612" y="+84" font-size="12" fill="black">=$10,000</text>
        <line x1="-17" y1="0" y2="0" x2="${8*len}" stroke="#7778" />
        ${docs.map(svg(idx)).join('')}
      </svg>
    </div>
    `).join('')}
  </body>
</html>
`;
  await fs.mkdir('visual', { recursive: true });
  await fs.writeFile('visual/timeline.html', html);
  console.log('finishing');
  await client.close();
})();

