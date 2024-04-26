import { MyDB } from "./lib/models/db";
import PAIRS from "./pairs.json";
import downloadEngine from "./lib/models/download-engine";
import { logger } from "./lib/utils";

let Pairs: MyDB[] = []

const init = async () => {
    Pairs = PAIRS.map(pair => new MyDB(pair.symbol, pair.min_historical_day))
}

process.on('SIGINT', async () => {
    logger.info('clean exit started')
    downloadEngine.shutDown()
    await Promise.allSettled(Pairs.map(pair => {
        return pair.db.close()
    }))
    logger.info('clean exit done')
    process.exit(0)
})


const main = async () => {    
    init()
    for (const pair of Pairs) {
        await pair.downloadSymbolArchives(pair)
    }
}

main()

