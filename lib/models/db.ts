import { Level } from "level"
import { ARCHIVE_FOLDER, DATABASES_PATH, MIN_TIME_FRAME } from "../constant"
import fs from 'fs'
import path from "path"
import { buildDateStr } from "../utils"
import downloadEngine, { DownloadEngine } from "./download-engine"

export class MyDB {
    public db: Level<string, string>

    constructor(public symbol: string, public minHistoricalDate: string) {
        fs.existsSync(DATABASES_PATH) || fs.mkdirSync(DATABASES_PATH, {recursive: true})
        this.db = new Level(`${DATABASES_PATH}/${symbol.toLowerCase()}`, { valueEncoding: 'json' })
    }

    isFullyInitialized = (timeFrame: number = MIN_TIME_FRAME) => this.isDateParsed(this.minHistoricalDate, timeFrame)

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

    downloadSymbolArchives = async (db: MyDB) => {
        let i = 1;
        const folderPath = path.join(ARCHIVE_FOLDER, db.symbol)
        !fs.existsSync(folderPath) && fs.mkdirSync(folderPath, { recursive: true })
    
        while (true){
            const date = buildDateStr(i)
            if (date < db.minHistoricalDate){
                break
            }
            const p = await db.isDateParsed(date)
            if (!p){
                const fileName = `${db.symbol}-trades-${date}.zip`;
                const fullPath = path.join(folderPath, fileName)
                downloadEngine.add(DownloadEngine.buildURL(db.symbol, date), fullPath)
            }
            i++
        }
    }

}
