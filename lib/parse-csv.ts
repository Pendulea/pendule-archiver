import fs from 'fs';
import { parse } from 'csv-parse'; // Ensure this is correctly imported
import moment from 'moment'
import AdmZip from 'adm-zip'
import { ITick, MIN_TIME_FRAME, storeCandles } from './candles';
import { MyDB } from './models/db';
import { countNewlines, largeNumberToShortString, safeAverage, safeMedian, timeFrameToLabel } from './utils';
import { ARCHIVE_FOLDER, IMPORT_TRADE_BATCH_SIZE } from './constant';

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

export const parseAndStoreZipArchive = async (db: MyDB, date: string, onUpdatePercentage?: (percent: number) => void): Promise<null | Error> => {
  const { symbol } = db
  try {
    const r = await db.isDateParsed(date)
    if (r || date < db.minHistoricalDate)
      return null
  } catch (error) {
    return error as Error
  }

  console.log(`Start parsing ${symbol} trades (${date})`)
  const path = `${ARCHIVE_FOLDER}/${symbol}/${symbol}-trades-${date}`;
  try {
    const err = unzipFile(`${path}.zip`, `${ARCHIVE_FOLDER}/${symbol}`)
    if (err)
      return err
  } catch (error) {
    return error as Error
  }

  let countLines = 0
  let input: fs.ReadStream
  try {
    countLines = await countNewlines(`${path}.csv`)
    input = fs.createReadStream(`${path}.csv`, {
      encoding: 'utf8',
      highWaterMark: 1024 * 1024
  });
  } catch (error) {
    return error as Error
  }

  const parser = parse({
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

  let countBatch = 0;
  let countCandlesStored = 0;
  let startProcessTime = Date.now();

  return new Promise(async (resolve, reject) => {
    let trades: ITrade[] = []
    let prevTimeBucket = 0;

    input.pipe(parser).on('data', async (trade: ITrade) => {
      const timeBucket = (trades.length + 1000) >= IMPORT_TRADE_BATCH_SIZE ? Math.floor(trade.timestamp / MIN_TIME_FRAME) * MIN_TIME_FRAME : 0;
      if (trades.length >= IMPORT_TRADE_BATCH_SIZE && timeBucket !== prevTimeBucket){
        parser.pause()
        const candles = aggregateTradesToCandles(trades, MIN_TIME_FRAME);
        const err = await storeCandles(db, candles, timeFrameToLabel(MIN_TIME_FRAME));
        if (err){
          err && resolve(err);
          return
        }
        trades = [trade];

        countCandlesStored += candles.size;
        countBatch++
        const totalTradeHandled = IMPORT_TRADE_BATCH_SIZE * countBatch
        const percentDone = Math.floor((totalTradeHandled / countLines) * 100)
        onUpdatePercentage && onUpdatePercentage(totalTradeHandled / countLines)
        const remainingTime = (Date.now() - startProcessTime) / totalTradeHandled * (countLines - totalTradeHandled)
        console.log(`${db.symbol} (${date}): PERCENT=${percentDone}%  PARSED=${largeNumberToShortString(countCandlesStored)} candles   SPEED=${largeNumberToShortString(totalTradeHandled / ((Date.now() - startProcessTime) / 1000))} trades/s   ETA=${moment.duration(remainingTime).humanize()}`)
        parser.resume()
      } else {
        trades.push(trade)
      }
      prevTimeBucket = timeBucket;

    }).on('end', async () => {
      if (trades.length > 0){
        const candles = aggregateTradesToCandles(trades, MIN_TIME_FRAME);
        const err = await storeCandles(db, candles, timeFrameToLabel(MIN_TIME_FRAME));
        err && resolve(err);
        countCandlesStored += candles.size;
      }
      await db.setDateAsParsed(date)
      fs.unlink(`${path}.csv`, () => null);
      const countTotalTrade = (IMPORT_TRADE_BATCH_SIZE * countBatch) + trades.length;

      console.log(`${db.symbol} (${date}): Successfully parsed ${largeNumberToShortString(countTotalTrade)} trades into ${largeNumberToShortString(countCandlesStored)} candles`)
      resolve(null)
    })
    .on('error', (error) => {
      fs.unlink(`${path}.csv`, () => null);
      resolve(error as Error)
    });
  })
}