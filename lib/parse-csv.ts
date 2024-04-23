import fs from 'fs';
import { parse } from 'csv-parse/sync'; // Ensure this is correctly imported

import AdmZip from 'adm-zip'
import { ITick, MIN_TIME_FRAME, storeCandles } from './candles';
import { MyDB } from './db';
import { safeAverage, safeMedian, timeFrameToLabel } from './utils';
import { ARCHIVE_FOLDER } from './constant';

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

  let candleTrades: ITrade[] = [];
  for (const trade of trades) {
    const timeBucket = Math.floor(trade.timestamp / interval) * interval;
    const candle = buckets.get(timeBucket);

    if (!candle){
      candleTrades = [trade];
      buckets.set(timeBucket, {
        open: trade.price,
        high: trade.price,
        low: trade.price,
        close: trade.price,
        volume_bought: trade.isBuyerMaker ? trade.quantity : 0,
        volume_sold: trade.isBuyerMaker ? 0 : trade.quantity,
        trade_count: 1,
        median_volume_bought: trade.quantity,
        average_volume_bought: trade.quantity,
        median_volume_sold: trade.quantity,
        average_volume_sold: trade.quantity,
        vwap: trade.price,
        standard_deviation: 0,
      });
    } else {
      candleTrades.push(trade);

      // Update existing candle
      candle.high = Math.max(candle.high, trade.price);
      candle.low = Math.min(candle.low, trade.price);
      candle.close = trade.price;
      candle.volume_bought += trade.isBuyerMaker ? trade.quantity : 0;
      candle.volume_sold += trade.isBuyerMaker ? 0 : trade.quantity;
      candle.trade_count += 1;

      // Prepare for median and average calculations
      const tradeVolumesBought = candleTrades.filter(t => t.isBuyerMaker).map(t => t.quantity);
      const tradeVolumesSold = candleTrades.filter(t => !t.isBuyerMaker).map(t => t.quantity);

      candle.median_volume_bought = safeMedian(tradeVolumesBought);
      candle.median_volume_sold = safeMedian(tradeVolumesSold);
      candle.average_volume_bought = safeAverage(tradeVolumesBought);
      candle.average_volume_sold = safeAverage(tradeVolumesSold);

      candle.vwap = (candle.vwap * (candle.trade_count - 1) + trade.price) / candle.trade_count;

      const meanPrice = tradeVolumesBought.concat(tradeVolumesSold).reduce((acc, qty) => acc + qty, 0) / (tradeVolumesBought.length + tradeVolumesSold.length);
      candle.standard_deviation = Math.sqrt(tradeVolumesBought.concat(tradeVolumesSold)
          .reduce((acc, qty) => acc + Math.pow(qty - meanPrice, 2), 0) / (tradeVolumesBought.length + tradeVolumesSold.length));
    }
  }
  
  return buckets;
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
    const err = await storeCandles(db, candles, timeFrameToLabel(MIN_TIME_FRAME));
    if (err) 
      return err

    await db.setDateAsParsed(date)
    console.log(`Parsed ${records.length.toLocaleString()} trades into ${candles.size.toLocaleString()} candles for ${symbol} (${date})`)
    return null
  } catch (error) {
    console.error('Failed to parse CSV:', error);
    return error as Error
  };
};