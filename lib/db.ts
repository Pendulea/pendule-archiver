import fs from 'fs'
import { Level } from 'level'
import { downloadSymbolArchives, getAllArchiveFiles, getOldestArchiveDayAge } from './archive-downloader'
import { parseAndStoreZipArchive } from './parse-csv'
import { extractDateFromTradeZipFile } from './utils'

const DATABASES_PATH = './databases'

export class MyDB {
    public db: Level<string, string>
    
    private _lastCSVDate: string | null = null
    constructor(public symbol: string, private minHistoricalDate: string) {
        fs.existsSync(DATABASES_PATH) || fs.mkdirSync(DATABASES_PATH, {recursive: true})
        const db = new Level(`${DATABASES_PATH}/${symbol.toLowerCase()}`, { valueEncoding: 'json' })
        this.db = db
    }

    init = async () => {
        const genesis = await this.getGenesisDate()
        if (!genesis) {
            await this.setGenesisDate(this.minHistoricalDate)
        } else {
            this._lastCSVDate = await this.getLastCSVDate()
        }
        await downloadSymbolArchives(this.symbol, this.minHistoricalDate)
        const allFiles = await getAllArchiveFiles(this.symbol)
        for (const file of allFiles){
            const d = extractDateFromTradeZipFile(file)
            if (d){
                const err = await parseAndStoreZipArchive(this, this.symbol, d)
                if (err){
                    throw err
                } else {
                    console.log(`Parsed and stored ${d} for ${this.symbol}`)
                }
            }
        }
    }

    private setGenesisDate = (date: string) => {
        return this.db.put('genesis', date)
    }
    
    public getGenesisDate = async () => {
        try {
            const genesis = await this.db.get('genesis')
            return genesis
        } catch (err: any) {
            if (err.message.includes('NotFound')) {
                return null
            }
            throw err
        }
    }

    public setLastCSVDate = (date: string) => {
        this._lastCSVDate = date
        return this.db.put('lastCSV', date)
    }

    public getLastCSVDate = async () => {
        try {
            if (this._lastCSVDate){
                return this._lastCSVDate
            }
            const lastCSV = await this.db.get('lastCSV')
            return lastCSV
        } catch (err: any) {
            if (err.message.includes('NotFound')) {
                return null
            }
            throw err
        }
    }
}
