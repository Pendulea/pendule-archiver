import { ITick, MIN_TIME_FRAME } from "./candles";
import { readdir } from 'fs/promises';
import { format } from 'date-fns';
import fs from'fs'
import moment from "moment";

export const safeMedian = (values: number[]) => {
    if (values.length === 0) return 0;
    const sorted = values.sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
  };
  
export const safeAverage = (values: number[]) => values.reduce((acc, cur) => acc + cur, 0) / (values.length || 1);
  

export const tickMapToJSONArray = (map: Map<number, ITick>) => {
    const arr: (ITick & { time: number })[] = []
    for (let [key, value] of map) {
        arr.push({
            ...value,
            time: key
        })
    }
    return arr
}

export function extractDateFromTradeZipFile(filename: string): string | null {
    // Regular expression to match the date in the filename
    const regex = /(\d{4}-\d{2}-\d{2})\.zip$/;
    const match = filename.match(regex);

    if (match && match[1]) {
        // If a match is found, return the date string
        return match[1];
    } else {
        // If no match is found, return null
        return null;
    }
}

export const timeFrameToLabel = (timeFrame: number): string => {
    if (timeFrame < MIN_TIME_FRAME) {
      throw new Error('Time frame is too small');
    }
    if (timeFrame % MIN_TIME_FRAME !== 0) {
      throw new Error(`Time frame must be a multiple of ${MIN_TIME_FRAME/ 1000} seconds`);
    }
  
    let seconds = Math.floor(timeFrame / 1000);
    let label = `${seconds}s`;
    if (seconds > 59 && seconds % 60 === 0){
      label = `${seconds / 60}m`;
    }
    if (seconds > 3599 && seconds % 3600 === 0){
      label = `${seconds / 3600}h`;
    }
    if (seconds > 86399 && seconds % 86400 === 0){
      label = `${seconds / 86400}d`;
    }
    if (seconds > 604799 && seconds % 604800 === 0){
      label = `${seconds / 604800}w`;
    }
    return label
}

export function getFileSize(filePath: string) {
  return fs.statSync(filePath).size
}

export const strDateToDate = (d: string) => {
    const date = new Date(d + "T00:00:00Z");
    // Return the UTC timestamp
    return date
}

export const formatDateStr = (d: Date) => {
    return format(new Date(d.toISOString().slice(0, -1)),'yyyy-MM-dd')
}

export const buildDateStr = (dAgo: number) => {
    let now = new Date()
    now.setDate(now.getDate() - dAgo);

    return format(new Date(now.toISOString().slice(0, -1)),'yyyy-MM-dd')
}


export async function sortFolderFiles(folderPath: string): Promise<string[]> {
    try {
        // Read directory contents
        const files = await readdir(folderPath);

        // Filter and sort ZIP files
        const zipFiles = files.filter(file => file.endsWith('.zip')).sort();
        return zipFiles;
    } catch (error) {
        console.error('Failed to read or sort files:', error);
        return [];
    }
}

export function countNewlines(filePath: string): Promise<number> {
  return new Promise((resolve, reject) => {
      let lineCount = 0;
      const stream = fs.createReadStream(filePath, { encoding: 'utf8', highWaterMark: 1024 * 1024});

      stream.on('data', (chunk: string) => {
          // Count the newlines in the current chunk
          lineCount += (chunk.match(/\n/g) || []).length;
      });

      stream.on('end', () => {
          resolve(lineCount);
      });

      stream.on('error', (err) => {
          reject(err);
      });
  });
}

export const largeBytesToShortString = (b: number) => {
    if (b >= 1_000_000_000) {
        return (b / 1_000_000_000).toFixed(2) + 'GB';
    }
    if (b >= 1_000_000) {
        return (b / 1_000_000).toFixed(1) + 'MB';
    }
    if (b >= 1_000) {
        return (b / 1_000).toFixed(0) + 'KB';
    }
    return b.toString() + 'B';
}

export const largeNumberToShortString = (n: number) => {
    if (n >= 1_000_000_000) {
        return (n / 1_000_000_000).toFixed(2) + 'B';
    }
    if (n >= 1_000_000) {
        return (n / 1_000_000).toFixed(1) + 'M';
    }
    if (n >= 1_000) {
        return (n / 1_000).toFixed(0) + 'K';
    }
    return n.toString();
}

export interface InspectablePromise<T> extends Promise<T> {
    isFulfilled: () => boolean;
    isRejected: () => boolean;
    isSettled: () => boolean;
    onFulfilled: () => void;
}

export function makeInspectable<T>(promise: Promise<T>): InspectablePromise<T> {
    let isFulfilled = false;
    let isRejected = false;

    const wrappedPromise = promise.then(
        (value: T) => {
            isFulfilled = true;
            return value;
        },
        (error: any) => {
            isRejected = true;
            throw error;
        }
    ) as InspectablePromise<T>;

    wrappedPromise.isFulfilled = () => isFulfilled;
    wrappedPromise.isRejected = () => isRejected;
    wrappedPromise.isSettled = () => isFulfilled || isRejected;

    return wrappedPromise;
}


export const accurateHumanize = (ms: number) => {
    if (ms < 1000) {
        return `${ms.toFixed(0)}ms`;
    }
    if (ms < 40_000) {
        return `${(ms / 1000).toFixed(1)}s`;
    }
    return moment.duration(ms).humanize();
}