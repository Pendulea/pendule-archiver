import { MIN_TIME_FRAME, storeNewTimeFrameCandles } from "../candles"
import { parseAndStoreZipArchive } from "../parse-csv"
import { InspectablePromise, makeInspectable } from "../utils"
import { MyDB } from "./db"

interface ITask {
    percent: number
    db: MyDB
    date: string
    started_at: number
    timeFrame?: number
    result: InspectablePromise<Error | null> | null
}

class Task {

    private data: ITask
    constructor(db: MyDB, date: string, timeFrame?: number){
        this.data = {
            db,
            date,
            started_at: 0,
            timeFrame,
            percent: 0,
            result: null
        }
    }

    db = () => this.data.db
    date = () => this.data.date
    timeFrame = () => this.data.timeFrame


    hasFinished = () => {
        const r = this.data.result
        if (r)
            return r.isSettled()
        return false
    }

    hasStarted = () => {
        return this.data.started_at > 0
    }

    isRunning = () => {
        return this.data.result && !this.hasFinished()
    }

    run = () => {
        if (this.hasStarted()){
            return
        }
        this.data.started_at = Date.now()
        const { db, date, timeFrame } = this.data

        if (!timeFrame || timeFrame === MIN_TIME_FRAME){
            this.data.result = makeInspectable(parseAndStoreZipArchive(db, date, (p: number) => {
                this.data.percent = p
            }))
        } else {
            this.data.result = makeInspectable(storeNewTimeFrameCandles(db, timeFrame, (p: number) => {
                this.data.percent = p
            }))
        }
    }


}

let _interval: NodeJS.Timeout | null = null

class Engine { 

    tasks: Task[] = []
    constructor(){
        _interval = setInterval(() => {
            this.run()
        }, 5000)
    }

    shutdown = () => {
        if (_interval){
            clearInterval(_interval)
        }
        console.log(`[TASK ENGINE] Shutting down...`)
    }

    hasRunningTask = () => {
        return this.tasks.some(t => t.isRunning())
    }

    containsTask = (db: MyDB, date: string, timeFrame: number) => {
        return this.tasks.some(task => task.db().symbol === db.symbol && task.date() === date && task.timeFrame() === timeFrame)
    }

    add = (db: MyDB, date: string, timeFrame: number = MIN_TIME_FRAME, priority?: boolean) => {
        if (timeFrame % MIN_TIME_FRAME !== 0) {
            throw new Error(`Timeframe must be a multiple of ${MIN_TIME_FRAME}`)
        }
        if (timeFrame < MIN_TIME_FRAME){
            throw new Error(`Timeframe must be greater than ${MIN_TIME_FRAME}`)
        }
        if (!this.containsTask(db, date, timeFrame)) {
            const task = new Task(db, date, timeFrame)
            if (!priority){
                this.tasks.push(task)                
            } else {
                this.tasks.unshift(task)
            }
        }
    }

    private async run () {
        if (this.hasRunningTask())
            return
        const first = this.tasks.filter(t => !t.hasStarted())[0]
        if (!first) 
            return
        first.run()
    }
}

const processEngine = new Engine()
export default processEngine