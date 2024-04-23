import { Level } from "level";
import { MyDB } from "./db";
import { safeAverage, safeMedian, timeFrameToLabel } from "./utils";

export const MIN_TIME_FRAME = 1_000;

export interface ITick {
    open: number; // open
    high: number; // high
    low: number; // low
    close: number; // close
    volume_bought: number; // volume buyed
    volume_sold: number; // volume sold
    trade_count: number; // trade count

    median_volume_bought: number; 
    average_volume_bought: number;
    median_volume_sold: number; 
    average_volume_sold: number;

    vwap: number; // volume weighted average price
    standard_deviation: number; // standard deviation
}

export const getEarliestTimeRecorded = async (db: Level<string, string>, label: string): Promise<number | null> => {
    try {
      const value = await db.get(`${label}:earliest`);
      return parseInt(value, 10);
    } catch (err: any) {
      if (err.message.includes('NotFound')) {
        return null;
      }
      throw err;
    }
}

export const updateEarliestTimeRecorded = async (db: Level<string, string>, earliestTime: number, label: string): Promise<null | Error> => {
    try {
      // Try to get the current earliest time
      const value = await db.get(`${label}:earliest`);
      const leastRecorded = parseInt(value, 10);
  
      // Check if the new time is earlier and update if it is
      if (earliestTime < leastRecorded) {
        await db.put(`${label}:earliest`, earliestTime.toString());
        // console.log(`Earliest ${label} candle updated to ${earliestTime}`);
      }
      return null; // Return null if successful
    } catch (err: any) {
      // Handle not found error separately
      if (err.message.includes('NotFound')) {
        // If no record exists, create one with the earliestTime
        await db.put(`${label}:earliest`, earliestTime.toString());
        // console.log(`Earliest ${label} candle set to ${earliestTime}`);
        return null;
      }
      return err; // Return error for further handling if necessary
    }
  };
  

export const storeNewTimeFrameCandles = async (db: MyDB, timeFrame: number): Promise<null | Error> => {
    const minLabel = timeFrameToLabel(MIN_TIME_FRAME);
    const minEarliest = await getEarliestTimeRecorded(db.db, timeFrameToLabel(MIN_TIME_FRAME));
    if (!minEarliest) {
      return null;
    }


    //check if the time frame is valid
    const newTF = timeFrameToLabel(timeFrame);
    const newTimeFrameSecs = Math.floor(timeFrame / 1000);
    
    let t0 = minEarliest;
    let t1 = t0 + newTimeFrameSecs
  
    const newCandles = new Map<number, ITick>();
    let prevTick: ITick | null = null;

    console.log(`Storing new candles for ${db.symbol} with time frame ${newTF}`);
    while (t0 < (Date.now() / 1000)){
      const candles = await getCandlesInDateRange(db.db, minLabel, t0, t1);
      if (candles.size > 0){
        let firstEntry, lastEntry;
        const values = candles.values()
        let entries = candles.entries();
        // Get the first entry
        firstEntry = entries.next().value;      
        // Continue iterating to find the last entry
        for (let entry of entries) 
            lastEntry = entry;
        // If there's only one entry, the first and last are the same
        lastEntry = lastEntry || firstEntry;
  
        const volumesBought = Array.from(candles.values()).map(c => c.volume_bought);
        const volumesSold = Array.from(candles.values()).map(c => c.volume_sold);

        const tick: ITick = {
          open: prevTick ? prevTick.close : firstEntry[1].open,
          high: Math.max(...Array.from(values).map(c => c.high)),
          low: Math.min(...Array.from(values).map(c => c.low)),
          close: lastEntry[1].c,
          volume_bought: volumesBought.reduce((acc, v) => acc + v, 0),
          volume_sold: volumesSold.reduce((acc, v) => acc + v, 0),
          trade_count: Array.from(candles.values()).reduce((acc, c) => acc + c.trade_count, 0),
          average_volume_bought: safeAverage(volumesBought),
          average_volume_sold: safeAverage(volumesSold),

          vwap: Array.from(candles.values()).reduce((acc, c) => acc + c.vwap * (c.volume_sold + c.volume_bought), 0) / Array.from(candles.values()).reduce((acc, c) => acc + (c.volume_sold + c.volume_bought), 0), // Corrected VWAP calculation
          standard_deviation: Math.sqrt(Array.from(candles.values()).reduce((acc, c) => acc + Math.pow(c.close - tick.vwap, 2), 0) / candles.size),
          median_volume_bought: safeMedian(volumesBought),
          median_volume_sold: safeMedian(volumesSold)
        }
        prevTick = tick;
        newCandles.set((t1 * 1000) - 1, tick);
      }
      t0 = t1;
      t1 = t0 + newTimeFrameSecs;
    }
  
    const err = await storeCandles(db, newCandles, newTF)
    if (err) {
      return err;
    }
    await db.setDateAsParsed(db.minHistoricalDate, timeFrame);
    return null
}

const stringifyTick = (tick: ITick): string => {
  return `${tick.open}|${tick.high}|${tick.low}|${tick.close}|${tick.volume_bought}|${tick.volume_sold}|${tick.trade_count}|${tick.median_volume_bought}|${tick.average_volume_bought}|${tick.median_volume_sold}|${tick.average_volume_sold}|${tick.vwap}|${tick.standard_deviation}`;
}

const parseTick = (str: string): ITick => {
  const [open, high, low, close, volume_bought, volume_sold, trade_count, median_volume_bought, average_volume_bought, median_volume_sold, average_volume_sold, vwap, standard_deviation] = str.split('|');
  return {
    open: parseFloat(open),
    high: parseFloat(high),
    low: parseFloat(low),
    close: parseFloat(close),
    volume_bought: parseFloat(volume_bought),
    volume_sold: parseFloat(volume_sold),
    trade_count: parseInt(trade_count),
    median_volume_bought: parseFloat(median_volume_bought),
    average_volume_bought: parseFloat(average_volume_bought),
    median_volume_sold: parseFloat(median_volume_sold),
    average_volume_sold: parseFloat(average_volume_sold),
    vwap: parseFloat(vwap),
    standard_deviation: parseFloat(standard_deviation)
  }
}


export const getCandlesInDateRange = async (db: Level<string, string>, label: string, t0: number, t1: number): Promise<Map<number, ITick>> => {
    const candles = new Map<number, ITick>();
  
    try {
      // Create a read stream filtered by the label prefix
      for await (const [key, value] of db.iterator({ 
        gte: `${label}:${t0}`,
        lt: `${label}:${t1}`
      })) {
        const timeInSeconds = parseInt(key.split(':')[1]);
        const tickData = parseTick(value);
        candles.set(timeInSeconds, tickData);
      }
    } catch (err) {
      console.error('Error fetching candles:', err);
      throw err;
    }
  
    return candles;
}

export async function storeCandles(db: MyDB, candles: Map<number, ITick>, label: string): Promise<null | Error> {
    let earliest = Number.MAX_SAFE_INTEGER;
    const batch = db.db.batch(); // Assuming `db.db` has a batch method available

    try {
        for (const [time, value] of candles.entries()) {
            const timeInSeconds = Math.floor(time / 1000);
            batch.put(`${label}:${timeInSeconds}`, stringifyTick(value)); // Add to batch
            if (timeInSeconds < earliest) 
                earliest = timeInSeconds;
        }
  
        await batch.write(); // Execute all batched operations
        const err = await updateEarliestTimeRecorded(db.db, earliest, label);
        if (err) 
            return err;
        // console.log(`Stored ${candles.size} ${label} candles for ${db.symbol}`);
        return null;
    } catch (err: any) {
        console.error('Failed to store candles:', err);
        return err;
    }
}

  