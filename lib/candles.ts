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
  
export const deleteTimeFrameCandles = async (db: MyDB, timeFrame: number): Promise<null | Error> => {
    const label = timeFrameToLabel(timeFrame);
    if (!(await db.isFullyInitialized())){
      return new Error(`Database ${db.symbol} is not fully initialized`);
    }
  
    if (timeFrame === MIN_TIME_FRAME){
      return new Error(`Cannot delete MIN_TIME_FRAME candles, just remove databases`);
    }

    if (!(await db.getTimeFrameList()).includes(timeFrame)){
      return null
    }
  
    try {
      const keysToDelete: string[] = [];
      for await (const [key, value] of db.db.iterator({ 
        gte: `${label}:`,
        lt: `${label};`
      })) {
        keysToDelete.push(key);
      }
  
      const batch = db.db.batch();
      for (const key of keysToDelete) {
        batch.del(key);
      }
  
      await batch.write();
      await db.db.del(`p:${timeFrame}:${db.minHistoricalDate}`);
      return null;
    } catch (err: any) {
      console.error('Error deleting candles:', err);
      return err;
    }
}


export const storeNewTimeFrameCandles = async (db: MyDB, timeFrame: number): Promise<null | Error> => {
    const minLabel = timeFrameToLabel(MIN_TIME_FRAME);
    if (!(await db.isFullyInitialized())){
      return new Error(`Database ${db.symbol} is not fully initialized`);
    }
    
    if (!(await db.getTimeFrameList()).includes(timeFrame)){
      return new Error(`Time frame ${timeFrame} not found in ${db.symbol} database`);
    } 

    if (await db.isDateParsed(db.minHistoricalDate, timeFrame)){
      return null
    }

    const minEarliest = await getEarliestTimeRecorded(db.db, timeFrameToLabel(MIN_TIME_FRAME));
    if (!minEarliest) {
      console.error(`No candles found for ${db.symbol} with time frame ${minLabel}`);
      process.exit(1);
    }

    //check if the time frame is valid
    const newTF = timeFrameToLabel(timeFrame);
    const newTimeFrameSecs = Math.floor(timeFrame / 1000);
    
    const d1 = new Date(minEarliest * 1000);
    d1.setDate(d1.getDate() + 1);
    d1.setHours(0, 0, 0, 0);


    let t0 = minEarliest;
    let t1 = Math.min(t0 + newTimeFrameSecs, Math.floor(d1.getTime() / 1000))
    console.log(minEarliest, d1.getTime() / 1000, t0, t1)

    const tMax = Date.now() / 1000;
  
    const newCandles = new Map<number, ITick>();
    let prevTick: ITick | null = null;

    console.log(`0% : building ${newTF.toUpperCase()} candles for ${db.symbol}`);
    let prevPercent = 0;
    while (t0 < tMax){
      const candles = await getCandlesInDateRange(db.db, minLabel, t0, t1);
      if (candles.size > 0){
        const values = Array.from(candles.values())

        const first = values[0];
        const last = values[values.length - 1];

        const volumesBought = values.map(c => c.volume_bought);
        const volumesSold = values.map(c => c.volume_sold);

        const tick: ITick = {
          open: prevTick ? prevTick.close : first.open,
          high: Math.max(...values.map(c => c.high)),
          low: Math.min(...values.map(c => c.low)),
          close: last.close,
          volume_bought: volumesBought.reduce((acc, v) => acc + v, 0),
          volume_sold: volumesSold.reduce((acc, v) => acc + v, 0),
          trade_count: values.reduce((acc, c) => acc + c.trade_count, 0),
          average_volume_bought: safeAverage(volumesBought),
          average_volume_sold: safeAverage(volumesSold),

          vwap: values.reduce((acc, c) => acc + c.vwap * (c.volume_sold + c.volume_bought), 0) / values.reduce((acc, c) => acc + (c.volume_sold + c.volume_bought), 0), // Corrected VWAP calculation
          standard_deviation: 0,
          median_volume_bought: safeMedian(volumesBought),
          median_volume_sold: safeMedian(volumesSold)
        }
        tick.standard_deviation = Math.sqrt(values.reduce((acc, c) => acc + Math.pow(c.close - tick.vwap, 2), 0) / candles.size)
        prevTick = tick;
        newCandles.set((t1 * 1000) - 1, tick);
      }
      const percent = Math.floor((t1 - minEarliest) / (tMax - minEarliest) * 100)
      if ((percent > prevPercent && percent % 5 === 0) || (t0 === minEarliest)){
        console.log(`${percent}% : building ${newTF.toUpperCase()} candles for ${db.symbol}`);
      }
      prevPercent = percent;
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

  