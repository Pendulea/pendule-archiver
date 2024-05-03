import { MIN_TIME_FRAME } from "../constant"
import fs from 'fs'
import path from "path"
import { buildDateStr, logger } from "../utils"
import downloadEngine from "./download-engine"
import { service } from "../rpc"
import axios from "axios"
import { format, parseISO } from "date-fns"

export class Symbol {

    static BuildPairID = (pairSymbol: string, future: boolean) => {
        return pairSymbol + (future ? '_futures' : '_spot')
    }

    constructor(private symbol: string, private minHistoricalDate: string, private future: boolean) {}


    public id = () => {
        return Symbol.BuildPairID(this.symbol, this.future)
    }

    public buildArchivePath = (date: string) => {
        const spotOrFuture = this.future ? 'FUTURE' : 'SPOT'
        const folderPath = path.join(global.ARCHIVE_DIR, this.symbol, spotOrFuture)
        !fs.existsSync(folderPath) && fs.mkdirSync(folderPath, { recursive: true })

        const fileName = `${this.symbol}-trades-${date}.zip`;
        return path.join(folderPath, fileName)
    } 

    public buildURL = (date: string) => {
        const formattedDate = format(parseISO(date), 'yyyy-MM-dd');
        const fileName = `${this.symbol}-trades-${formattedDate}.zip`;
        return `https://data.binance.vision/data/spot/daily/trades/${this.symbol}/${fileName}`;
    }

    public isFullyInitialized = () => this.isDateParsed(this.minHistoricalDate)

    public checkSymbol = async () => {
        try {
            const response = await axios.head(this.buildURL(this.minHistoricalDate));
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

    public isDateParsed = async (date: string) => {
        try {
            const r = await service.request('IsDateParsed', {
                symbol: this.symbol,
                date,
                timeframe: MIN_TIME_FRAME
            }) as {exist: boolean}
            return r.exist
        } catch (error) {
            return true
        }
   }

    downloadSymbolArchives = async () => {
        let i = 1;
        while (true){
            const date = buildDateStr(i)
            if (date < this.minHistoricalDate){
                break
            }
            const p = await this.isDateParsed(date)
            if (!p){
                downloadEngine.add(this, date)
            }
            i++
        }
    }
}