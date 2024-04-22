import { MyDB } from "./lib/db";
import express, { Request } from 'express';
import cors from 'cors';
import morgan from 'morgan'
import { getCandlesInDateRange } from "./lib/candles";
import { strDateToDate, tickMapToJSONArray } from "./lib/utils";

const db = new MyDB('CTSIUSDT', '2020-04-23')

const app = express();
app.use(express.json());

app.use(morgan('tiny', { skip: (req: Request) => {
    if (req.path.startsWith('/media') || req.method === 'OPTIONS')
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
    const t0 = strDateToDate(date as string)
    const t1 = t0.getTime() + 24 * 60 * 60 * 1000

    const ret = await getCandlesInDateRange(db.db, ts as string, t0.getTime() / 1000, t1 / 1000)
    res.json(tickMapToJSONArray(ret))
})

const main = async () => {
    await db.init()
    await app.listen({
        port: 8080,
        host: '0.0.0.0',
    }, () => {
        console.log('[API]', 'Listening on port', 8080)
    });
}

main()

