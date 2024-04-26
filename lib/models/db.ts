import { MIN_TIME_FRAME } from "../constant"
import fs from 'fs'
import path from "path"
import { buildDateStr } from "../utils"
import downloadEngine, { DownloadEngine } from "./download-engine"
import { ManyLevelGuest, ManyLevelHost } from 'many-level'
import rocksdb from 'rocksdb'

export class MyDB {
    public db: rocksdb

    constructor(public symbol: string, public minHistoricalDate: string) {
        const db = rocksdb(path.join(global.DB_DIR, symbol.toLowerCase()))
        this.db = db
        this.db.open({readOnly: true}, (err) => {
            if (err){
                console.error(err)
                process.exit(0)
            }
        })
    }

    isFullyInitialized = (timeFrame: number = MIN_TIME_FRAME) => this.isDateParsed(this.minHistoricalDate, timeFrame)

    public isDateParsed = async (date: string, timeFrame: number = MIN_TIME_FRAME) => {
        while (this.db.status !== 'open'){
            await new Promise(resolve => setTimeout(resolve, 300))
        }

        return new Promise<boolean>((resolve, reject) => {
            this.db.get(`p:${timeFrame}:${date}`, (err, value) => {
                if (err) {
                    if (err.message.includes('NotFound')) {
                        resolve(false)
                    } else {
                        reject(err)
                    }
                } else {
                    resolve(true)
                }
            })
        })
    }

    downloadSymbolArchives = async (db: MyDB) => {
        let i = 1;
        const folderPath = path.join(global.ARCHIVE_DIR, db.symbol)
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
