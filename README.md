# Evaluating the Impact of Chain-of-Thought Length in LLMs on Stock Price Movement Predictions

**Objective:** Assess whether large language models (LLMs) with extended chain-of-thought reasoning improve predictions of stock price movements following earnings announcements.

**Background:** Public companies listed on NASDAQ are required to publish quarterly earnings reports. Publicly traded companies on NASDAQ release quarterly earnings reports, which significantly impact stock prices. These earnings are typically announced either after market close (around 4:05 PM) or before market open (around 9:00 AM).

For earnings announced after market close, stock prices react almost instantly in after-hours trading, depending on whether earnings exceed or miss analysts' expectations. Overnight, extended-hour trading further adjusts the price, influenced by investor sentiment, option market signals, and liquidity.

At the 9:30 AM market open, another sharp movement occurs. The key question is whether this movement continues the after-hours trend or reverses, and whether longer chain-of-thought reasoning in LLMs can improve the accuracy of such predictions.

**Methodology:**

1. **Data Collection:** Gather historical earnings reports, corresponding stock price data, and analyst estimates.
2. **LLM Configuration:** Develop multiple LLMs with varying chain-of-thought lengths.
3. **Input Preparation:** Convert financial data into textual format suitable for LLM processing.
4. **Prediction and Evaluation:** Analyze each model's predictive accuracy regarding stock price movements post-earnings announcements.

**References:**

Mackintosh, Phil. Earnings Announcements Sliced and Diced [nasdaq.com](https://www.nasdaq.com/articles/earnings-announcements-sliced-and-diced)

## Steps to reproduce the results

1. Install npm packages and pip packages:

    ```bash
    npm ci
    pip install -r requirements.txt
    ```

2. Setting up environment variables, put it in file `.env`

    - `FINNHUB_API_KEY`
    - `MONGO_URL`
    - `DATABENTO_API_KEY`
    - `GEMINI_API_KEY`
    - `GROQ_API_KEY`
    - `OLLAMA_URL`

3. Run the JavaScript scripts using node.js in the specific order. Note that on Linux you can use:

    ```bash
    run-parts --regex '\.js$' scripts
    ```

    To run them manually, invoke:

    1. `node scripts/01-download-earnings.js`: Download 1-month company earnings data from Finnhub

        - writes to MongoDB collection `earnings.earnings`

    2. `node scripts/02-download-index.js`: Download 1-month stock index data from Databento

        - writes to MongoDB timeseries `earnings.stock_indexes`

    3. `node scripts/03-download-symbols.js`: Download stock symbol data from Databento

        - writes to MongoDB collection `earnings.symbols`

    4. `node scripts/04-download-ohlcv.js`: Download historical stock price (bid, ask, trade) data from Databento, including EXT hours

        - writes to MongoDB collection `earnings.prices`

    5. `node scripts/05-unify-symbols.js`: Tranform downloaded stock symbol data to filter out actively traded U.S. stocks.

        - reads from MongoDB collection `earnings.symbols`
        - writes to MongoDB collection `earnings.symbol_ids`

    6. `node scripts/06-transform-price.js`: Transform downloaded stock price data into MongoDB timeseries for faster, easier processing

        - reads from MongoDB collection `earnings.prices`
        - writes to MongoDB timeseries `earnings.prices_cleaned`

    7. `node scripts/07-transform-earnings.js`: Combine earnings data with stock prices data, computing key stock metrics

        - reads from MongoDB collection `earnings.earnings`
        - reads from MongoDB timeseries `earnings.stock_indexes`
        - reads from MongoDB timeseries `earnings.prices_cleaned`
        - writes to MongoDB collection `earnings.earnings_cleaned`

    8. `node scripts/08-generate-descriptions.js`: For each earnings incident, generate a comprehensive, textual report briefing the historical stock price movement as well as intraday/after-market/pre-market trading activities before and after the earnings release

        - reads from MongoDB collection `earnings.earnings_cleaned`
        - writes to MongoDB collection `earnings.earnings_cleaned`

    9. `node scripts/09-combine-descriptions.js`: Part all valid earnings data into examples (n=3) and test (n=120), then compile LLM prompts for making predictions on each of the test data

        - reads from MongoDB collection `earnings.earnings_cleaned`
        - writes to files in `desc/<symbol>_<quarter>*.txt`

    10. `node scripts/10-query-llms.js`: For each LLM prompt, invoke many different LLM API to get answer (Gemini + GroqCloud + Ollama)

        - reads from files in `desc/<symbol>_<quarter>*.txt`
        - writes to MongoDB collection `earnings.llm_outputs`

    11. `node scripts/11-import-output.js`: Some LLM does not have an open API or are too expensive - we need to manually collect the data, form a `*.tsv` file, and then feed to MongoDB

        - reads from the file specified by the command line arguments
        - writes to MongoDB collection `earnings.llm_outputs`

    12. `node scripts/12-parse-order.js`: For each LLM output, parse the requested trade order, and output the net profit from such trade

        - reads from MongoDB collection `earnings.llm_outputs`
        - writes to MongoDB collection `earnings.llm_outputs`

    13. `node scripts/13-visualize-timeline.js`: For each LLM, organize profit/loss into a timeline for easier visualization

        - reads from MongoDB collection `earnings.llm_outputs`
        - writes to file `visual/timeline.html`

    14. `node scripts/14-export-csv.js`: Organize data into CSV for easier python processing

        - reads from MongoDB collection `earnings.llm_outputs`
        - writes to file `visual/data.csv`
        - writes to file `visual/data.json`

4. Execute the Jupyter Notebook file `scripts/15-data-visualizations.ipynb`
