require('dotenv').config();

import downloadEngine from "./lib/models/download-engine";
import { logger } from "./lib/utils";
import fs from 'fs'
import path from "path";
import { service }  from './lib/rpc'
import minicall from 'minicall'
import { handlePairParsingJSON } from "./lib/pairs";


const cleanup = async () => {
    logger.info('cleaning up...')
    process.off('SIGINT', cleanup)
    service.stop()
    await downloadEngine.shutDown()
    fs.unwatchFile(process.env.PAIRS_PATH || '')
    logger.info('clean exit done')
    process.exit(0)
    
}

const initPathEnv = () => {
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
}


new minicall({
    time: ["06:00:00", "12:00:00", "18:00:00"], //Based on UTC time 
    execute: () => handlePairParsingJSON()
}).start()


const main = async () => { 
    initPathEnv()
    handlePairParsingJSON()
    const PAIRS_PATH = process.env.PAIRS_PATH || ''

    fs.watch(PAIRS_PATH, (eventType) => eventType === 'change' && handlePairParsingJSON())
}

process.on('SIGINT', async () => {
    await cleanup()
})
main()


