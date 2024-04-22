import { ITick } from "./candles";

/**
   * Calculate RSI for an array of ticks.
   * Assumes ticks are in chronological order.
   * @param ticks ITick[]
   * @param period number The number of periods to use in RSI calculation, typically 14.
   * @returns number The RSI value
   */
  function calculateRSI(ticks: ITick[], period: number = 14): number {
    if (ticks.length < period) {
      throw new Error("Not enough data to compute RSI");
    }
  
    let gains = 0;
    let losses = 0;
  
    // Calculate gains and losses
    for (let i = 1; i < period; i++) {
      const change = ticks[i].c - ticks[i - 1].c;
      if (change > 0) {
        gains += change;
      } else {
        losses -= change;  // losses are positive numbers
      }
    }
  
    const averageGain = gains / period;
    const averageLoss = losses / period;
  
    // Calculate RS and RSI
    const rs = averageGain / averageLoss;
    const rsi = 100 - (100 / (1 + rs));
  
    return rsi;
}
