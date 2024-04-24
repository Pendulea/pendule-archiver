import { MyDB } from "./lib/models/db";
import express, { Request } from 'express';
import cors from 'cors';
import morgan from 'morgan'
import { getCandlesInDateRange } from "./lib/candles";
import { strDateToDate, tickMapToJSONArray } from "./lib/utils";
import PAIRS from "./pairs.json";
import { IncomingMessage, Server, ServerResponse } from "http";
import { parseAndStoreZipArchive } from "./lib/parse-csv";
import downloadEngine from "./lib/models/download-engine";
import processEngine from "./lib/models/process-engine";
import { kill } from "process";

let Pairs: MyDB[] = []

const init = async () => {
    Pairs = PAIRS.map(pair => new MyDB(pair.symbol, pair.min_historical_day))
    // const p = Pairs.find(p => p.symbol === 'BTCUSDT')
    // if (p){
    //     await parseAndStoreZipArchive(p, `2023-03-21`)
    // }
}

const app = express();
let server: Server<typeof IncomingMessage, typeof ServerResponse>
app.use(express.json());

app.use(morgan('tiny', { skip: (req: Request) => {
    if (req.method === 'OPTIONS')
        return true
    return false
}}));

app.use(function(req, res, next) {
    res.header("Access-Control-Allow-Headers", "Access-Control-Max-Age, Access-Control-Allow-Origin, Origin, X-Requested-With, Content-Type, Accept, day");
    res.header("Access-Control-Allow-Origin", "*"); 
    res.header("Access-Control-Max-Age", "7200")
    res.header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
    next();
});
app.use(cors());

app.get('/pair/:pair/:timeframe', async (req, res) => {
    const date = req.headers.day 
    const ts = req.params.timeframe
    if (!date || !ts) {
        res.status(400).send('Missing date or timeframe')
        return
    }
    const pairDB = Pairs.find(p => p.symbol === req.params.pair.toUpperCase())
    if (!pairDB) {
        res.status(400).send('Invalid pair')
        return
    }
    if (pairDB.minHistoricalDate > date) {
        res.status(400).send(`Pair ${pairDB.symbol} does not have data for ${date}, the earliest date is ${pairDB.minHistoricalDate}`)
        return
    }
    
    const t0 = strDateToDate(date as string)
    const t1 = t0.getTime() + 24 * 60 * 60 * 1000

    const ret = await getCandlesInDateRange(pairDB.db, ts as string, t0.getTime() / 1000, t1 / 1000)
    res.json(tickMapToJSONArray(ret))
})

//add time frame to the pair
app.post('/pair/:pair/:timeframe', async (req, res) => {
    const ts = parseInt(req.params.timeframe)
    if (ts === 0 || isNaN(ts)) {
        res.status(400).send('Invalid time frame')
        return
    }
    const pairDB = Pairs.find(p => p.symbol === req.params.pair.toUpperCase())
    if (!pairDB) {
        res.status(400).send('Invalid pair')
        return
    }

    const err = await pairDB.addTimeFrame(ts)
    if (err) {
        res.status(500).send(err.message)
        return
    }
    res.sendStatus(200)
})

app.delete('/pair/:pair/:timeframe', async (req, res) => {
    const ts = parseInt(req.params.timeframe)
    if (ts === 0 || isNaN(ts)) {
        res.status(400).send('Invalid time frame')
        return
    }
    const pairDB = Pairs.find(p => p.symbol === req.params.pair.toUpperCase())
    if (!pairDB) {
        res.status(400).send('Invalid pair')
        return
    }
    await pairDB.removeTimeFrame(ts)
    res.sendStatus(200)
})

process.on('SIGINT', async () => {
    console.log('clean exit started')
    downloadEngine.shutDown()
    processEngine.shutdown()
    while(true){
        if (!processEngine.hasRunningTask())
            break
        await new Promise(resolve => setTimeout(resolve, 1000))
        console.log('waiting for tasks to finish')
    }
    console.log('all tasks done')
    await Promise.allSettled(Pairs.map(pair => {
        return pair.db.close()
    }))
    console.log('all db closed')
    await new Promise((resolve) => {
        server.close(() => {
            resolve(null)
        })
    })
    console.log('server closed')
    console.log('clean exit done')
    kill(process.pid, 'SIGKILL')
})


const main = async () => {    
    init()
    server = app.listen({
        port: 8080,
        host: '0.0.0.0',
    }, () => {
        console.log('[API]', 'Listening on port', 8080)
    });
}

main()

