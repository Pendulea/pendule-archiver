import { MIN_TIME_FRAME } from "../constant"
import fs from 'fs'
import path from "path"
import { buildDateStr, logger } from "../utils"
import downloadEngine, { DownloadEngine } from "./download-engine"
import { service } from "../rpc"
import axios from "axios"

export class Symbol {

    constructor(public symbol: string, public minHistoricalDate: string) {}

    isFullyInitialized = (timeFrame: number = MIN_TIME_FRAME) => this.isDateParsed(this.minHistoricalDate, timeFrame)

    public checkSymbol = async () => {
        try {
            const response = await axios.head(DownloadEngine.buildURL(this.symbol, this.minHistoricalDate));
            if (response.status === 200) {
                return true
            }
            return false
        } catch (error: any) {
            logger.error('Error checking symbol', {
                symbol: this.symbol,
                error: error.message
            })
            return false
        }
    }

    public isDateParsed = async (date: string, timeFrame: number = MIN_TIME_FRAME) => {
        try {
            const r = await service.request('IsDateParsed', {
                symbol: this.symbol,
                date,
                timeframe: timeFrame
            }) as {exist: boolean}
            return r.exist
        } catch (error) {
            return true
        }
   }

    downloadSymbolArchives = async () => {
        let i = 1;
        const folderPath = path.join(global.ARCHIVE_DIR, this.symbol)
        !fs.existsSync(folderPath) && fs.mkdirSync(folderPath, { recursive: true })
    
        while (true){
            const date = buildDateStr(i)
            if (date < this.minHistoricalDate){
                break
            }
            const p = await this.isDateParsed(date)
            if (!p){
                const fileName = `${this.symbol}-trades-${date}.zip`;
                const fullPath = path.join(folderPath, fileName)
                downloadEngine.add(DownloadEngine.buildURL(this.symbol, date), fullPath)
            }
            i++
        }
    }

}
