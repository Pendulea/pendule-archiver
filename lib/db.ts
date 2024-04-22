import fs from 'fs'
import { Level } from 'level'
import { downloadSymbolArchives } from './archive-downloader'
import { parseAndStoreZipArchive } from './parse-csv'
import { extractDateFromTradeZipFile } from './utils'

const DATABASES_PATH = './databases'

class queue { 

    private _isRunning = false
    toRun: {db: MyDB, date: string}[] = []
    constructor(){}

    add = (db: MyDB, date: string) => {
        this.toRun.push({db, date})
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


export class MyDB {
    public db: Level<string, string>
    
    constructor(public symbol: string, public minHistoricalDate: string) {
        fs.existsSync(DATABASES_PATH) || fs.mkdirSync(DATABASES_PATH, {recursive: true})
        const db = new Level(`${DATABASES_PATH}/${symbol.toLowerCase()}`, { valueEncoding: 'json' })
        this.db = db
    }

    init = async () => {
        await downloadSymbolArchives(this, async (date: string) => {
            engine.add(this, date)
        })
    }

    public setDateAsParsed = (date: string) => {
        return this.db.put(`parsed:${date}`, '1')
    }

    public isDateParsed = async (date: string) => {
        try {
            await this.db.get(`parsed:${date}`)
            return true
        } catch (err: any) {
            if (err.message.includes('NotFound')) {
                return false
            }
            throw err
        }
    }
}
