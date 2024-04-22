import fs from 'fs';
import { parse } from 'csv-parse/sync'; // Ensure this is correctly imported
import { Level } from 'level';
import { ARCHIVE_FOLDER } from './archive-downloader';

import AdmZip from 'adm-zip'
import { ITick, MIN_TIME_FRAME, storeCandles } from './candles';
import { MyDB } from './db';
import { timeFrameToLabel } from './utils';

interface ITrade {
  tradeId: number;
  price: number;
  quantity: number;
  total: number;
  timestamp: number;
  isBuyerMaker: boolean;
  isBestMatch: boolean;
}

function aggregateTradesToCandles(trades: ITrade[], interval: number) {
  const buckets = new Map<number, ITick>();

  for (const trade of trades) {
    const timeBucket = Math.floor(trade.timestamp / interval) * interval;
    const candle = buckets.get(timeBucket);
    if (candle){
      candle.h = Math.max(candle.h, trade.price);
      candle.l = Math.min(candle.l, trade.price);
      candle.c = trade.price;
      candle.vb = (candle.vb || 0) + (trade.isBuyerMaker ? trade.quantity : 0);
      candle.vs = (candle.vs || 0) + (trade.isBuyerMaker ? 0 : trade.quantity);
      candle.tc = (candle.tc || 0) + 1;
    } else {
      buckets.set(timeBucket, {
        o: trade.price,
        h: trade.price,
        l: trade.price,
        c: trade.price,
        vb: trade.isBuyerMaker ? trade.quantity : 0,
        vs: trade.isBuyerMaker ? 0 : trade.quantity,
        tc: 1
      });
    }
  }
  
  return buckets;
}


const aggregateAndStore = async (db: MyDB, trades: ITrade[]) => {
  const candles = aggregateTradesToCandles(trades, MIN_TIME_FRAME);
  const err = await storeCandles(db, candles, timeFrameToLabel(MIN_TIME_FRAME));
  if (err) {
    console.error('Error storing candles:', err);
  }
  return err
}

function unzipFile(zipPath: string, outputPath: string): null | Error{
    try {
        const zip = new AdmZip(zipPath);
        zip.extractAllTo(outputPath, true)
        return null
    } catch (error) {
      return error as Error
    }
}

export const parseAndStoreZipArchive = async (db: MyDB, date: string) => {
  const { symbol } = db
  const r = await db.isDateParsed(date)
  if (r || date < db.minHistoricalDate){
    return null
  }
  const path = `${ARCHIVE_FOLDER}/${symbol}/${symbol}-trades-${date}`;
  const err = unzipFile(`${path}.zip`, `${ARCHIVE_FOLDER}/${symbol}`)
  if (err)
    return err
  try {
    const fileContent = fs.readFileSync(`${path}.csv`, 'utf-8');
    const records: ITrade[] = parse(fileContent, {
      delimiter: ',',
      columns: ['tradeId', 'price', 'quantity', 'total', 'timestamp', 'isBuyerMaker', 'isBestMatch'],
      skip_empty_lines: true,
      from_line: 1,
      cast: (value, context) => {
        switch (context.column) {
          case 'tradeId': return parseInt(value);
          case 'price': return parseFloat(value);
          case 'quantity': return parseFloat(value);
          case 'total': return parseFloat(value);
          case 'timestamp': return parseInt(value);
          case 'isBuyerMaker': return value === 'True';
          case 'isBestMatch': return value === 'True';
          default: return value;
        }
      }
  })
    fs.unlink(`${path}.csv`, () => null);

    const candles = aggregateTradesToCandles(records, MIN_TIME_FRAME);
    const err2 = await storeCandles(db, candles, timeFrameToLabel(MIN_TIME_FRAME));
    if (err2) {
      return err2
    }
    await db.setDateAsParsed(date)
    console.log(`Parsed ${records.length} trades into ${candles.size} candles for ${symbol} (${date})`)
    return null    
  } catch (error) {
    console.error('Failed to parse CSV:', error);
    return error as Error
  };
};