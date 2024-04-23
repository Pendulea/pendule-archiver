import fs from 'fs'
import { Level } from 'level'
import { downloadSymbolArchives } from './archive-downloader'
import { parseAndStoreZipArchive } from './parse-csv'
import { DATABASES_PATH } from './constant'
import { MIN_TIME_FRAME } from './candles'

class queue { 

    private _isRunning = false
    toRun: {db: MyDB, date: string, timeFrame?: number}[] = []
    constructor(){}

    interrupt = () => {
        this.toRun = []
    }

    canStop = () => {
        return this.toRun.length === 0 && !this._isRunning
    }

    add = (db: MyDB, date: string, timeFrame: number = MIN_TIME_FRAME) => {
        if (timeFrame % MIN_TIME_FRAME !== 0) {
            throw new Error(`Timeframe must be a multiple of ${MIN_TIME_FRAME}`)
        }
        this.toRun.push({db, date, timeFrame})
        this.run()
    }

    private async run () {
        if (this._isRunning) {
            return
        }
        this._isRunning = true
        const first = this.toRun.shift()
        if (!first) {
            this._isRunning = false
            return
        }
        const err = await parseAndStoreZipArchive(first.db, first.date)
        if (err) {
            console.error(err)
        }
        this._isRunning = false
        this.run()
    }
}

const engine = new queue()

process.on('SIGINT', async () => {
    engine.interrupt()
    while (!engine.canStop()) {
        await new Promise(resolve => setTimeout(resolve, 300))
    }
});



export class MyDB {
    public db: Level<string, string>
    
    constructor(public symbol: string, public minHistoricalDate: string) {
        fs.existsSync(DATABASES_PATH) || fs.mkdirSync(DATABASES_PATH, {recursive: true})
        const db = new Level(`${DATABASES_PATH}/${symbol.toLowerCase()}`, { valueEncoding: 'json' })
        this.db = db
    }

    getTimeFrameList = async () => {
        try {
            const ret = await this.db.get(`timeframes`)
            const list = JSON.parse(ret) as number[]
            return list
        } catch (err: any) {
            if (err.message.includes('NotFound')) {
                return []
            }
            throw err
        }
    }

    isFullyInitialized = (timeFrame: number = MIN_TIME_FRAME) => this.isDateParsed(this.minHistoricalDate, timeFrame)

    addTimeFrame = async (timeFrame: number) => {
        if (timeFrame % MIN_TIME_FRAME !== 0) {
            throw new Error(`Timeframe must be a multiple of ${MIN_TIME_FRAME}`)
        }
        if (timeFrame <= MIN_TIME_FRAME){
            throw new Error(`Timeframe must be greater than ${MIN_TIME_FRAME}`)
        }
        const list = await this.getTimeFrameList()
        if (!list.includes(timeFrame)) {
            list.push(timeFrame)
            return this.db.put(`timeframes`, JSON.stringify(list))
        }
    }

    removeTimeFrame = async (timeFrame: number) => {
        const list = await this.getTimeFrameList()
        const index = list.indexOf(timeFrame)
        if (index !== -1) {
            list.splice(index, 1)
            return this.db.put(`timeframes`, JSON.stringify(list))
        }
    }

    init = async () => {
        await downloadSymbolArchives(this, async (date: string) => {
            engine.add(this, date)
        })
    }

    public setDateAsParsed = (date: string, timeFrame: number = MIN_TIME_FRAME) => {
        return this.db.put(`p:${timeFrame}:${date}`, '1')
    }

    public isDateParsed = async (date: string, timeFrame: number = MIN_TIME_FRAME) => {
        try {
            await this.db.get(`p:${timeFrame}:${date}`)
            return true
        } catch (err: any) {
            if (err.message.includes('NotFound')) {
                return false
            }
            throw err
        }
    }
}
