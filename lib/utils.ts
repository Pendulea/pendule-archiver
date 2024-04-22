import { MIN_TIME_FRAME } from "./candles";
import { readdir } from 'fs/promises';
import { format } from 'date-fns';

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