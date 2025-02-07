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
