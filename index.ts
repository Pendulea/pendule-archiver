require('dotenv').config();

import { Symbol } from "./lib/models/symbol";
import downloadEngine from "./lib/models/download-engine";
import { logger } from "./lib/utils";
import fs from 'fs'
import path from "path";
import { service }  from './lib/rpc'
import minicall from 'minicall'

let Pairs: Symbol[] = []

interface IPair {
    binance: boolean
    symbol0: string
    symbol1: string
    min_historical_day: string
}

const getBinanceSymbols = (p: IPair) => {
    if (p.binance) {
        return p.symbol0.toUpperCase() + p.symbol1.toUpperCase()
    }
    return null
}


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

const handlePairParsingJSON = async () => {
    const PAIRS_PATH = process.env.PAIRS_PATH || ''
    if (!fs.existsSync(PAIRS_PATH)){
        logger.error('pairs.json file not found')
        process.exit(0)
    }
    const data = fs.readFileSync(PAIRS_PATH, 'utf8')
    let json: IPair[] = []
    try {
        json = JSON.parse(data) as IPair[]
    } catch (error) {
        logger.error('pairs.json file is not a valid json file')
        return
    }
    

    Pairs = []
    for (const pair of json) {
        const binancePair = getBinanceSymbols(pair)

        if (!!pair.min_historical_day.trim() && !!binancePair){
            const exist = Pairs.find(p => p.symbol === binancePair)
            if (!exist){
                const s = new Symbol(binancePair, pair.min_historical_day)
                const symbolFound = await s.checkSymbol()
                if (symbolFound){
                    Pairs.push(s)
                    await s.downloadSymbolArchives()
                } else {
                    logger.error('Symbol not found', {
                        symbol: binancePair
                    })
                }
            }
        
        } else if (pair.binance) {
            logger.warn('Invalid pair in pairs.json file', {
                pair: JSON.stringify(pair)
            })
        }
    }



    logger.info('Pairs initialized', {
        active: Pairs.length,
        inactive: json.length - Pairs.length
    })
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


