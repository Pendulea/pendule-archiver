import fs from 'fs'
import { Level } from 'level'
import { downloadSymbolArchives } from './archive-downloader'
import { parseAndStoreZipArchive } from './parse-csv'
import { DATABASES_PATH } from './constant'
import { MIN_TIME_FRAME, deleteTimeFrameCandles, storeNewTimeFrameCandles } from './candles'

class queue { 

    private _isRunning = false
    private _stop = false
    private _downloadCountParallel = 0
    private _shutdownCallbacks: (() => void)[] = []


    toRun: {db: MyDB, date: string, timeFrame?: number}[] = []
    constructor(){}

    increaseDownloadCount = () => {
        this._downloadCountParallel++
    }
    decrementDownloadCount = () => {
        this._downloadCountParallel--
    }

    countRunningDownloads = () => this._downloadCountParallel

    onShutdown = (cb: () => void) => {
        this._shutdownCallbacks.push(cb)
    }

    shutdown = () => {
        this.toRun = []
        this._stop = true
        for (const cb of this._shutdownCallbacks) {
            cb()
        }
    }

    isShuttingDown = () => this._stop

    isQueuingDBTasks = (db: MyDB) => {
        return this.toRun.some(task => task.db.symbol === db.symbol)
    }

    canStop = () => {
        return this.toRun.length === 0 && !this._isRunning && this._downloadCountParallel === 0
    }

    containsTask = (db: MyDB, date: string, timeFrame: number) => {
        return this.toRun.some(task => task.db.symbol === db.symbol && task.date === date && task.timeFrame === timeFrame)
    }

    add = (db: MyDB, date: string, timeFrame: number = MIN_TIME_FRAME) => {
        if (timeFrame % MIN_TIME_FRAME !== 0) {
            throw new Error(`Timeframe must be a multiple of ${MIN_TIME_FRAME}`)
        }
        if (timeFrame < MIN_TIME_FRAME){
            throw new Error(`Timeframe must be greater than ${MIN_TIME_FRAME}`)
        }
        if (!this.containsTask(db, date, timeFrame)) {
            this.toRun.push({db, date, timeFrame})
            this.run()
        }
    }

    private async run () {
        if (this._isRunning || this._stop) {
            return
        }
        this._isRunning = true
        const first = this.toRun.shift()
        if (!first) {
            this._isRunning = false
            return
        }
        if (!first.timeFrame || first.timeFrame === MIN_TIME_FRAME){
            const err = await parseAndStoreZipArchive(first.db, first.date)
            if (err) {
                console.error(err)
            }
        } else {
            const err = await storeNewTimeFrameCandles(first.db, first.timeFrame)
            if (err) {
                console.error(err)
            }
        }

        this._isRunning = false
        this.run()
    }
}

export const engine = new queue()

process.on('SIGINT', async () => {
    engine.shutdown()
    while (!engine.canStop()) {
        const downloads = engine.countRunningDownloads()
        if (downloads > 0) {
            console.log(`Waiting for ${downloads} downloads to finish`)
        } else{
            console.log(`Waiting for 1 task to finish`)
        }
        await new Promise(resolve => setTimeout(resolve, 1000))
    }

})

export class MyDB {
    public db: Level<string, string>
    private _isInitializing = false

    constructor(public symbol: string, public minHistoricalDate: string) {
        fs.existsSync(DATABASES_PATH) || fs.mkdirSync(DATABASES_PATH, {recursive: true})
        this.db = new Level(`${DATABASES_PATH}/${symbol.toLowerCase()}`, { valueEncoding: 'json' })
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

    isInitializing = () => this._isInitializing || engine.isQueuingDBTasks(this)

    isFullyInitialized = (timeFrame: number = MIN_TIME_FRAME) => this.isDateParsed(this.minHistoricalDate, timeFrame)

    addTimeFrame = async (timeFrame: number) => {
        if (timeFrame % MIN_TIME_FRAME !== 0) {
            return new Error(`Timeframe must be a multiple of ${MIN_TIME_FRAME}`)
        }
        if (timeFrame <= MIN_TIME_FRAME){
            return new Error(`Timeframe must be greater than ${MIN_TIME_FRAME}`)
        }
        const list = await this.getTimeFrameList()
        if (!list.includes(timeFrame)) {
            list.push(timeFrame)
            await this.db.put(`timeframes`, JSON.stringify(list))
            if (!this.isInitializing()){
                engine.add(this, this.minHistoricalDate, timeFrame)
            }
        }
        return null
    }

    removeTimeFrame = async (timeFrame: number) => {
        const err = await deleteTimeFrameCandles(this, timeFrame)
        if (!err){        
            const list = await this.getTimeFrameList()
            const index = list.indexOf(timeFrame)
            if (index !== -1) {
                list.splice(index, 1)
                return this.db.put(`timeframes`, JSON.stringify(list))
            }
        }
    }

    init = async () => {
        this._isInitializing = true
        await downloadSymbolArchives(this, async (date: string) => {
            engine.add(this, date)
        })
        this._isInitializing = false
        const timeFrames = await this.getTimeFrameList()
        for (const timeFrame of timeFrames) {
            engine.add(this, this.minHistoricalDate, timeFrame)
        }
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
