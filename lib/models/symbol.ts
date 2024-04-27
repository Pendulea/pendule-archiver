import { MIN_TIME_FRAME } from "../constant"
import fs from 'fs'
import path from "path"
import { buildDateStr } from "../utils"
import downloadEngine, { DownloadEngine } from "./download-engine"
import { service } from "../rpc"

export class Symbol {

    constructor(public symbol: string, public minHistoricalDate: string) {}

    isFullyInitialized = (timeFrame: number = MIN_TIME_FRAME) => this.isDateParsed(this.minHistoricalDate, timeFrame)

    public isDateParsed = async (date: string, timeFrame: number = MIN_TIME_FRAME) => {
        try {
            const r = await service.request('IsDateParsed', {
                symbol: this.symbol,
                date,
                timeframe: timeFrame
            }) as {exist: boolean}
            return r.exist
        } catch (error) {
            console.log(error)
            return true
        }
   }

    downloadSymbolArchives = async (symbol: Symbol) => {
        let i = 1;
        const folderPath = path.join(global.ARCHIVE_DIR, symbol.symbol)
        !fs.existsSync(folderPath) && fs.mkdirSync(folderPath, { recursive: true })
    
        while (true){
            const date = buildDateStr(i)
            if (date < symbol.minHistoricalDate){
                break
            }
            const p = await symbol.isDateParsed(date)
            if (!p){
                const fileName = `${symbol.symbol}-trades-${date}.zip`;
                const fullPath = path.join(folderPath, fileName)
                downloadEngine.add(DownloadEngine.buildURL(symbol.symbol, date), fullPath)
            }
            i++
        }
    }

}
