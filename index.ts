require('dotenv').config();

import { Symbol } from "./lib/models/symbol";
import downloadEngine from "./lib/models/download-engine";
import { logger } from "./lib/utils";
import fs from 'fs'
import path from "path";
import { set } from "date-fns";
import { service }  from './lib/rpc'

let Pairs: Symbol[] = []
let shutdownRequested = false;

const cleanup = async () => {
    if (shutdownRequested) {
        return
    }
    shutdownRequested = true
    service.stop()
    downloadEngine.shutDown()
    logger.info('clean exit done')
}


const main = async () => { 
    const PAIRS_PATH = process.env.PAIRS_PATH || ''
    const ARCHIVES_DIR = process.env.ARCHIVES_DIR || ''
    const DATABASES_DIR = process.env.DATABASES_DIR || ''

    if (!ARCHIVES_DIR || !fs.existsSync(ARCHIVES_DIR)) {
        logger.error('archives directory not found')
        process.exit(0)
    } else {
        logger.info('archives directory set', {
            path: ARCHIVES_DIR
        })
        global.ARCHIVE_DIR = path.join(ARCHIVES_DIR);
    }
    if (!DATABASES_DIR || !fs.existsSync(DATABASES_DIR)) {
        logger.error('databases directory not found')
        process.exit(0)
    } else {
        logger.info('databases directory set', {
            path: DATABASES_DIR
        })
        global.DB_DIR = path.join(DATABASES_DIR);
    }

    //check if valid json file
    try {
        if (!fs.existsSync(PAIRS_PATH)) {
            logger.error('pairs.json file not found')
            process.exit(0)
        }
        const data = fs.readFileSync(PAIRS_PATH, 'utf8')
        Pairs = JSON.parse(data).map(pair => {
            if (pair.symbol && pair.min_historical_day) {
                return new Symbol(pair.symbol, pair.min_historical_day)
            } else {
                logger.error('pairs.json file is not valid')
                process.exit(0)
            }
        })
        logger.info('pairs.json file loaded', {
            path: PAIRS_PATH,
            count: Pairs.length
        })
    } catch (error) {
        console.log(error)
        logger.error('pairs.json file is not a valid json file')
        process.exit(0)
    }

    for (const pair of Pairs) {
        await pair.downloadSymbolArchives(pair)
    }
}

process.on('SIGINT', async () => {
    await cleanup()
})
main()


