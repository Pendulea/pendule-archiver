import axios, { AxiosResponse } from "axios"
import { format, parseISO } from "date-fns"
import fs from "fs"
import { InspectablePromise, accurateHumanize, largeBytesToShortString, logger, makeInspectable } from "../utils"
import { MAX_NO_RESPONSE_TIMEOUT, MAX_PARALLEL_DOWNLOAD, MIN_INTERVAL_DOWNLOAD_STATUS } from "../constant"

interface IDownload {
    url: string
    path: string
    size_downloaded: number
    started_at: number
    end_at: number
    last_update: number
    total_size: number
    controller: AbortController | null
    result: InspectablePromise<DownloadResult> | null
    onDownloaded?: () => void
}

type DownloadResult = {
    status: 'success' | 'error';
    message: string;
    code: number; // HTTP status code, included in case of error
};


class Download {
    private data: IDownload
    constructor(url: string, path:string, onDownloaded?: () => void){
        this.data = {
            url,
            path,
            size_downloaded: 0,
            started_at: 0,
            end_at: 0,
            last_update: 0,
            total_size: 0,
            controller: null,
            result: null,
            onDownloaded
        }
    }


    id = () => this.data.url.replace('https://data.binance.vision/data/spot/daily/trades/', '')


    isBlank = () => {
        return !this.hasCachedDownload() && this.data.controller === null && this.data.result === null
    }

    isPathCached = () => {
        return fs.existsSync(this.data.path)
    }

    hasCachedDownload = () => {
        return this.data.total_size === 0 && this.data.started_at > 0 && this.data.size_downloaded === 0 && this.data.last_update > 0 && this.data.end_at > 0 && !!this.data.result
    }

    hasNoUpdates = () => {
        return !this.isBlank() && !this.hasCachedDownload() && this.isDownloading() && (Date.now() - this.data.last_update) > MAX_NO_RESPONSE_TIMEOUT
    }

    isDownloading = () => {
        return this.hasStarted() && !this.hasDownloaded() && !this.hasCachedDownload()
    }

    abort = () => {
        if (this.data.controller){    
            try {
                this.data.controller.abort()
            } catch(e){
                console.error(e)
            }
            if (!this.hasDownloaded()){
                fs.unlink(this.data.path, () => null)
            }
            this.data.controller = null
            this.data.result = null
            this.data.total_size = 0
            this.data.size_downloaded = 0
            this.data.started_at = 0
            this.data.end_at = 0
            this.data.last_update = 0
        }
    }

    estimatedTimeLeft = () => {
        if (this.isBlank() || this.hasCachedDownload()){
            return 0
        }
        if (this.hasStarted()){
            return (this.fileSize() - this.downloadedFileSize()) / (this.downloadedFileSize() / (this.downloadTime()))
        }
        return 0
    }

    printStatus = () => {
        if (this.isDownloading()){
            const eta = this.estimatedTimeLeft()
            if (eta){
                logger.log('info', `downloading ${this.id()}`, {
                    progress: this.percentString(),
                    done: largeBytesToShortString(this.downloadedFileSize()),
                    left: largeBytesToShortString(this.fileSize() - this.downloadedFileSize()),
                    speed: largeBytesToShortString(this.downloadedFileSize() / (this.downloadTime() / 1000)),
                    eta: accurateHumanize(eta)
                })
            }
        }
    }

    url = () => this.data.url

    downloadTime = () => {
        if (this.hasCachedDownload() || this.isBlank()){
            return 0
        }
        if (this.hasDownloaded()){
            return this.data.end_at - this.data.started_at
        }
        if (this.hasStarted()){
            return Date.now() - this.data.started_at
        }
        return 0
    }

    fileSize = () => {
        return this.data.total_size
    }

    downloadedFileSize = () => {
        return this.data.size_downloaded
    }

    hasStarted = () => {
        return this.data.started_at > 0
    }

    percentString = () => {
        if (this.isBlank()){
            return '0%'
        }
        if (this.hasCachedDownload()){
            return '100%'
        }
        if (this.hasStarted()){
            const n = this.downloadedFileSize() / this.data.total_size
            if (isNaN(n))
                return '0%'
            return `${Math.floor(n * 100)}%`
        }
        return '0%'
    }

    hasDownloaded = () => {
        const r = this.data.result
        return r ? r.isSettled() : false
    }

    getResultIfDone = async () => {
        if (this.hasDownloaded()){
            const r = await this.data.result
            if (r)
                return r
        }
        return null
    }

    start = async (callback?: () => void) => {
        const { onDownloaded } = this.data
        if (this.hasStarted()){
            return
        }

        this.data.started_at = Date.now()
        this.data.last_update = Date.now()
        if (this.isPathCached()){
            this.data.end_at = Date.now();
            this.data.result = makeInspectable(new Promise((resolve) => {
                resolve({ status: 'success', message: 'File downloaded successfully.', code: 200 })
                callback && callback()
                onDownloaded && onDownloaded()
            }))
            return
        }

        this.data.controller = new AbortController();
        let response: AxiosResponse<any, any> | null = null
        try {
            response = await axios({
                url: this.url(),
                method: 'GET',
                responseType: 'stream',
                signal: this.data.controller.signal,
                validateStatus: (status) => status >= 200 && status < 500
            });
        } catch (error: any){
            if (error.name === 'AbortError') {
                logger.log('info', `successfully aborted ${this.id()}`)
                fs.unlinkSync(this.data.path)
            } else {
                this.data.end_at = Date.now()
                this.data.last_update = Date.now()
                this.data.result = makeInspectable(new Promise(resolve => resolve({ status: 'error', message: error.message, code: 500 })))
            }
            return
        }
        if (response){
            if (response.status !== 200){
                this.data.end_at = Date.now()
                this.data.last_update = Date.now()
                this.data.result = makeInspectable(new Promise(resolve => resolve({ status: 'error', message: '', code: response?.status as number })))
                return
            }
        }

        this.data.total_size = parseInt(response?.headers['content-length']);

        response?.data.on('data', (chunk: any) => {
            this.data.size_downloaded += chunk.length
            if (Date.now() - this.data.last_update > MIN_INTERVAL_DOWNLOAD_STATUS){
                this.data.last_update = Date.now();
                this.printStatus()
            }
        })
        this.data.result = makeInspectable(new Promise<DownloadResult>((resolve) => {
            const writer = response?.data.pipe(fs.createWriteStream(this.data.path));
            writer.on('finish', () => {
                this.data.end_at = Date.now();
                this.data.last_update = Date.now();

                logger.log('info', `downloaded ${this.id()}`, {
                    time: accurateHumanize(this.downloadTime()),
                })
                resolve({ status: 'success', message: 'File downloaded successfully.', code: 200 })
                callback && callback()
                onDownloaded && onDownloaded()
            });
            writer.on('error', (e) => {
                logger.log('error', `error downloading ${this.id()}`, {
                    error: JSON.stringify(e)
                })
                resolve({ status: 'error', message: 'Error writing file.', code: 500 })
            });
        }))
    }
}


let _interval: NodeJS.Timeout | undefined = undefined
let _i = 0

export class DownloadEngine {

    static buildURL = (symbol: string, date: string) => {
        const formattedDate = format(parseISO(date), 'yyyy-MM-dd');
        const fileName = `${symbol}-trades-${formattedDate}.zip`;
        return `https://data.binance.vision/data/spot/daily/trades/${symbol}/${fileName}`;
    }

    private _countDownloaded = 0
    private downloads: Download[] = []
    private _pauseUntil = 0
    private _pauseTimeout: NodeJS.Timeout | undefined = undefined

    constructor(){
        _interval = setInterval(() => {
            if(_i % 20 === 0){
                this.printStatus()
                _i = 0
            }
            this.run()
            _i++
        }, 500)

    }

    printStatus = () => {
        if (this.downloads.length > 0){
            logger.log('info', 'Downloader status', {
                done: this._countDownloaded,
                pending: this.downloads.filter(d => d.isBlank()).length,
                downloading: this.downloads.filter(d => d.isDownloading()).length > 0 ? 'yes' : 'no',
                paused: this._pauseUntil > Date.now() ? 'yes' : 'no'
            })
            this.downloads.forEach(d => d.printStatus())
        }
    }


    shutDown = async () => {
        this.downloads.forEach(d => this.remove(d.url()))
        this.downloads = []
        clearTimeout(this._pauseTimeout)
        clearInterval(_interval)
        logger.log('info', 'shutting down download engine...')
    }

    pause = (seconds: number) => {
        this._pauseUntil = Date.now() + seconds * 1000
        clearTimeout(this._pauseTimeout)
        this._pauseTimeout = setTimeout(this.run, seconds * 1000)
    }

    add = (url: string, path: string, onDownloaded?: () => void) => {
        const d = this.downloads.find(d => d.url() === url)
        if (!d){
            const d = new Download(url, path, onDownloaded)
            this.downloads.push(d)
        }
    }

    remove = (url: string, reschedule = false) => {
        const d = this.downloads.find(d => d.url() === url)
        if (d){
            d.abort()
            this.downloads = this.downloads.filter(d => d.url() !== url)
            if (reschedule){
                this.downloads.push(d)
            }
        }
    }

    private handleDone = async () => {
        const finished = this.downloads.filter(d => d.hasDownloaded())
        for (const d of finished){
            const r = await d.getResultIfDone()
            if (r){
                const { code, message } = r
                if (code === 404 || code === 200){
                    this.remove(d.url())
                } 
                else if (code === 429){
                    this.remove(d.url(), true)
                    this.pause(60)
                } 
                else {
                    logger.log('error', `Error downloading ${d.url()}`, {
                        code,
                        message
                    })
                    this.remove(d.url(), true)
                    this.pause(30)                    
                }
            }
        }
    }

    run = () => {        
        if (this._pauseUntil > Date.now()){
            return
        }
        const dlOnes = this.downloads.filter(d => d.isDownloading())
        if (dlOnes.length >= MAX_PARALLEL_DOWNLOAD){
            return
        }

        this.downloads.forEach(d => d.isDownloading() && d.hasNoUpdates() && this.remove(d.url(), true))
        this.handleDone()

        const inactive = this.downloads.find(d => !d.hasStarted())
        if (inactive){
            inactive.start(() => {
                this._countDownloaded++
            })
        }
    }
}

const downloadEngine = new DownloadEngine()
export default downloadEngine