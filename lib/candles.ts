import { Level } from "level";
import { MyDB } from "./db";
import { timeFrameToLabel } from "./utils";

export const MIN_TIME_FRAME = 15_000;

export interface ITick {
    o: number; // open
    h: number; // high
    l: number; // low
    c: number; // close
    vb: number; // volume buyed
    vs: number; // volume sold
    tc: number; // trade count
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
        console.log(`Earliest ${label} candle updated to ${earliestTime}`);
      }
      return null; // Return null if successful
    } catch (err: any) {
      // Handle not found error separately
      if (err.message.includes('NotFound')) {
        // If no record exists, create one with the earliestTime
        await db.put(`${label}:earliest`, earliestTime.toString());
        console.log(`Earliest ${label} candle set to ${earliestTime}`);
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
    while (t0 < (Date.now() / 1000)){
      const candles = await getCandlesInDateRange(db.db, minLabel, t0, t1);
      if (candles.size > 0){
        let firstEntry, lastEntry;
        let entries = candles.entries();
        // Get the first entry
        firstEntry = entries.next().value;      
        // Continue iterating to find the last entry
        for (let entry of entries) 
            lastEntry = entry;
        // If there's only one entry, the first and last are the same
        lastEntry = lastEntry || firstEntry;
  
        const tick: ITick = {
          o: prevTick ? prevTick.c : firstEntry[1].o,
          h: Math.max(...Array.from(candles.values()).map(c => c.h)),
          l: Math.min(...Array.from(candles.values()).map(c => c.l)),
          c: lastEntry[1].c,
          vb: Array.from(candles.values()).reduce((acc, c) => acc + c.vb, 0),
          vs: Array.from(candles.values()).reduce((acc, c) => acc + c.vs, 0),
          tc: Array.from(candles.values()).reduce((acc, c) => acc + c.tc, 0)
        }
        prevTick = tick;
        newCandles.set((t1 * 1000) - 1, tick);
      }
      t0 = t1;
      t1 = t0 + newTimeFrameSecs;
    }
  
    return storeCandles(db, newCandles, newTF)
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
        const tickData: ITick = JSON.parse(value);
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
            batch.put(`${label}:${timeInSeconds}`, JSON.stringify(value)); // Add to batch
            if (timeInSeconds < earliest) 
                earliest = timeInSeconds;
        }
  
        await batch.write(); // Execute all batched operations
        const err = await updateEarliestTimeRecorded(db.db, earliest, label);
        if (err) 
            return err;
        console.log(`Stored ${candles.size} ${label} candles for ${db.symbol}`);
        return null;
    } catch (err: any) {
        console.error('Failed to store candles:', err);
        return err;
    }
}

  