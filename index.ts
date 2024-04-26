import { MyDB } from "./lib/models/db";
import downloadEngine from "./lib/models/download-engine";
import { logger } from "./lib/utils";
import yargs from 'yargs'
import { hideBin } from 'yargs/helpers'
import fs from 'fs'
import path from "path";

let Pairs: MyDB[] = []

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
    const argv = yargs(hideBin(process.argv))
        .option('pairs_path', {
            alias: 'pairs',
            type: 'string',
            description: 'Path to the pairs.json file',
            default: './pairs.json',
        })
        .option('archive_path', {
            alias: 'archive',
            type: 'string',
            description: 'Directory path for archives',
        })
        .option('database_path', {
            alias: 'db',
            type: 'string',
            description: 'Path to the database directory',
        })
        .argv; // Use .argv directly to get the parsed arguments

    if (!argv.archive_path || !fs.existsSync(argv.archive_path)) {
        logger.error('archive path not found')
        process.exit(0)
    } else {
        logger.info('archive folder set', {
            path: argv.archive_path
        })
        global.ARCHIVE_DIR = path.join(argv.archive_path);
    }
    if (!argv.database_path || !fs.existsSync(argv.database_path)) {
        logger.error('database path not found')
        process.exit(0)
    } else {
        logger.info('database folder set', {
            path: argv.database_path
        })
        global.DB_DIR = path.join(argv.database_path);
    }


    //check if valid json file
    try {
        if (!fs.existsSync(argv.pairs_path)) {
            logger.error('pairs.json file not found')
            process.exit(0)
        }
        const data = fs.readFileSync(argv.pairs_path, 'utf8')
        Pairs = JSON.parse(data).map(pair => {
            if (pair.symbol && pair.min_historical_day) {
                return new MyDB(pair.symbol, pair.min_historical_day)
            } else {
                logger.error('pairs.json file is not valid')
                process.exit(0)
            }
        })
        logger.info('pairs.json file loaded', {
            path: argv.pairs_path,
            count: Pairs.length
        })
    } catch (error) {
        logger.error('pairs.json file is not a valid json file')
        process.exit(0)
    }

    for (const pair of Pairs) {
        await pair.downloadSymbolArchives(pair)
    }
}

main()


