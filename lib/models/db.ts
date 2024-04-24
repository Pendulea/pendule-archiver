import { Level } from "level"
import { DATABASES_PATH, MIN_TIME_FRAME } from "../constant"
import fs from 'fs'
import engine from "./process-engine"
import { deleteTimeFrameCandles } from "../candles"
import { downloadSymbolArchives } from "../archive-downloader"

export class MyDB {
    public db: Level<string, string>

    constructor(public symbol: string, public minHistoricalDate: string) {
        fs.existsSync(DATABASES_PATH) || fs.mkdirSync(DATABASES_PATH, {recursive: true})
        this.db = new Level(`${DATABASES_PATH}/${symbol.toLowerCase()}`, { valueEncoding: 'json' })

        downloadSymbolArchives(this).then(() => {
            this.getTimeFrameList().then((timeFrames) => {
                for (const timeFrame of timeFrames) {
                    engine.add(this, this.minHistoricalDate, timeFrame)
                }
            })
        })
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
            return new Error(`Timeframe must be a multiple of ${MIN_TIME_FRAME}`)
        }
        if (timeFrame <= MIN_TIME_FRAME){
            return new Error(`Timeframe must be greater than ${MIN_TIME_FRAME}`)
        }
        const list = await this.getTimeFrameList()
        if (!list.includes(timeFrame)) {
            list.push(timeFrame)
            await this.db.put(`timeframes`, JSON.stringify(list))
            engine.add(this, this.minHistoricalDate, timeFrame, true)
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
