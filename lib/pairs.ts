import fs from 'fs'
import { logger } from './utils'
import { Symbol } from './models/symbol'

let Pairs: Symbol[] = []

export interface IPair {
    binance: boolean
    symbol0: string
    symbol1: string
    min_historical_day: string
    futures: boolean
}

export const resetPairs = () => { 
    Pairs = []
}

const buildBinanceSymbols = (p: IPair) => {
    if (isBinanceValid(p)) 
        return p.symbol0.toUpperCase() + p.symbol1.toUpperCase()
    return null
}

const isBinanceValid = (p: IPair) => {
    return p.binance && p.symbol0.trim() && p.symbol1.trim()
}

const pairErrorFilter = (p: IPair) => {
    if (!p.min_historical_day.trim()) {
        return 'min_historical_day is required'
    }

    if (p.binance) {
        if (!p.symbol0.trim() || !p.symbol1.trim()) {
            return 'symbol0 and symbol1 are required for binance pairs'
        }

        const symb1 = p.symbol1.toUpperCase()
        const allowedPairs = ['USDT', 'USDC', 'BUSD']
        if (!allowedPairs.includes(symb1)){
            return `pair ${p.symbol1} not allowed for symbol1: only ${allowedPairs.join(', ')}`
        }
    }

    return null
}

export const handlePairParsingJSON = async () => {
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

    resetPairs()
    for (const pair of json) {
        const binancePair = buildBinanceSymbols(pair)

        const error = pairErrorFilter(pair)
        if (error){
            logger.error('Invalid pair in pairs.json file', {
                pair: JSON.stringify(pair),
                error
            })
            continue
        } else if (binancePair){
            const exist = Pairs.find(p => p.setID() === Symbol.BuildSetID(binancePair, pair.futures))
            if (!exist){
                const s = new Symbol(binancePair, pair.min_historical_day, pair.futures)
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
        }
    }

    logger.info('Pairs initialized', {
        active: Pairs.length,
        inactive: json.length - Pairs.length
    })
}