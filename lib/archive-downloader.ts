import axios from 'axios';

import { format, parseISO } from 'date-fns';
import path from 'path'
import fs from 'fs'
import { buildDateStr}  from './utils';
import { MyDB } from './db';
import { ARCHIVE_FOLDER } from './constant';


type DownloadResult = {
    status: 'success' | 'error';
    message: string;
    code: number; // HTTP status code, included in case of error
};

async function downloadFile(url: string, path: string): Promise<DownloadResult> {
    try {
        const response = await axios({
            url,
            method: 'GET',
            responseType: 'stream'
        });

        // Use a new promise to handle the streaming and writing process
        return new Promise<DownloadResult>((resolve) => {
            const writer = response.data.pipe(fs.createWriteStream(path));
            writer.on('finish', () => resolve({ status: 'success', message: 'File downloaded successfully.', code: 200 }));
            writer.on('error', () => resolve({ status: 'error', message: 'Error writing file.', code: 500 }));
        });
    } catch (error: any) {
        if (axios.isAxiosError(error)) {
            // Handle different status codes with specific messages
            if (error.response) {
                return {
                    status: 'error',
                    message: `Failed to download file: server responded with status code ${error.response.status}`,
                    code: error.response.status
                };
            } else {
                return {
                    status: 'error',
                    message: 'No response received from the server.',
                    code: 500
                };
            }
        } else {
            // Non-Axios error
            return {
                status: 'error',
                message: `An unexpected error occurred: ${error.message}`,
                code: 500
            };
        }
    }
}

// Main function to download all archives from a given start date to today
export async function downloadArchive(date: string, symbol: string, folderPath: string) {

    const formattedDate = format(parseISO(date), 'yyyy-MM-dd');
    const fileName = `${symbol}-trades-${formattedDate}.zip`;
    const url = `https://data.binance.vision/data/spot/daily/trades/${symbol}/${fileName}`;

    const fullPath = path.join(folderPath, fileName)

    console.log(`Downloading ${fileName}...`);
    const r = await downloadFile(url, fullPath);
    if (r.code !== 200){
        // console.error(`Failed to download ${fileName}: ${r.message}`);
    } else {
        console.log(`${fileName} downloaded successfully.`);
    }
    return r
}


const downloadTree = async (db: MyDB, dAgo: {count: number}, onNewArchiveFound: (date: string) => void) => {
    const { symbol } = db
    while (true){
        const formattedDate = buildDateStr(dAgo.count);
        if (formattedDate < db.minHistoricalDate){
            return 404
        }
        const fileName = `${symbol}-trades-${formattedDate}.zip`;
        const fullP = `${ARCHIVE_FOLDER}/${symbol}/${fileName}`
        if (fs.existsSync(fullP)){
            onNewArchiveFound(formattedDate)
            dAgo.count++
            continue
        }
        const r = await downloadArchive(formattedDate, symbol, `${ARCHIVE_FOLDER}/${symbol}`)
        if (r.code !== 200){
            return r.code
        } else if (r.code === 200){
            onNewArchiveFound(formattedDate)
            dAgo.count++
            await new Promise(resolve => setTimeout(resolve, 500))
        }
    }
}

// export const getAllArchiveFiles = async (symbol: string) => {
//     const folderPath = `${ARCHIVE_FOLDER}/${symbol}`
//     const allExistingFiles = await sortFolderFiles(folderPath)
//     return allExistingFiles
// }

// export const getOldestArchiveDayAge = async (symbol: string) => {
//     const allExistingFiles = await getAllArchiveFiles(symbol)

//     const oldestFileOfArchiveDownloaded = allExistingFiles[0] || null
//     if (oldestFileOfArchiveDownloaded){
//         const extractedDate = extractDateFromTradeZipFile(oldestFileOfArchiveDownloaded)
//         if (!extractedDate) 
//             throw new Error('Failed to extract date from oldest file of archive downloaded')

//         const n = (new Date(buildDateStr(1)).getTime() - new Date(extractedDate).getTime()) / DAY_MS
//         return n + 1
//     }
//     return 0
// }


export const downloadSymbolArchives = async (db: MyDB, onNewArchiveFound: (date: string) => void) => {
    const { symbol } = db

    const folderPath = `${ARCHIVE_FOLDER}/${symbol}`

    fs.existsSync(folderPath) || fs.mkdirSync(folderPath, {recursive: true})

    const dAgo = {count: 1}
    while (true){
        const status = await downloadTree(db, dAgo, onNewArchiveFound)
        if (status === 404){
            console.log('No more archives to download.')
            break
        }
        if (status === 429){
            console.log('Rate limit reached. Waiting 1 minute...')
            await new Promise(resolve => setTimeout(resolve, 60_000))
        }
        if (status !== 200){
            console.error(`Failed to download ${status}`)
            await new Promise(resolve => setTimeout(resolve, 30_000))
        }
    }

}
