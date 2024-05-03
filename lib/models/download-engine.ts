import axios, { AxiosResponse } from "axios"
import fs from "fs"
import { accurateHumanize, extractDateFromTradeZipFile, extractSymbolFromTradeZipFile, largeBytesToShortString, logger } from "../utils"
import { MAX_NO_RESPONSE_TIMEOUT, MAX_PARALLEL_DOWNLOAD, MIN_INTERVAL_DOWNLOAD_STATUS } from "../constant"
import { green } from "colorette"
import { Symbol } from "./symbol"

interface IDownload {
    url: string
    path: string
    size_downloaded: number
    started_at: number
    end_at: number
    last_update: number
    total_size: number
    id: string
    controller: AbortController | null
}

type DownloadResult = {
    status: 'success' | 'error';
    message: string;
    code: number; // HTTP status code, included in case of error
};


class Download {
    private data: IDownload
    constructor(url: string, path:string, id: string){
        this.data = {
            url,
            path,
            size_downloaded: 0,
            started_at: 0,
            end_at: 0,
            last_update: 0,
            total_size: 0,
            id,
            controller: null,
        }
    }


    id = () => this.data.id.slice()

    hasStarted = () => {
        return this.data.started_at > 0
    }

    isBlank = () => {
        return !this.hasCachedDownload() && this.data.controller === null
    }

    isPathCached = () => {
        return fs.existsSync(this.data.path)
    }

    hasCachedDownload = () => {
        return this.data.total_size === 0 && this.data.started_at > 0 && this.data.size_downloaded === 0 && this.data.last_update > 0 && this.data.end_at > 0
    }

    hasNoUpdates = () => {
        return !this.isBlank() && !this.hasCachedDownload() && this.isDownloading() && (Date.now() - this.data.last_update) > MAX_NO_RESPONSE_TIMEOUT
    }

    isDownloading = () => {
        return this.hasStarted() && !this.hasDownloaded() && !this.hasCachedDownload()
    }

    hasDownloaded = () => {
        return this.data.end_at > 0 && this.data.total_size > 0 && this.data.size_downloaded === this.data.total_size
    }


    abort = () => {
        if (this.data.controller){    
            try {
                this.data.controller.abort()
            } catch(e){
                console.error(e)
            }
            if (this.fileSize() > 0 && !this.hasDownloaded()){
                fs.unlinkSync(this.data.path)
            }
            this.data.controller = null
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

    start = async () => {
        if (this.hasStarted()){
            return
        }

        this.data.started_at = Date.now()
        this.data.last_update = Date.now()
        if (this.isPathCached()){
            this.data.end_at = Date.now();
            return new Promise<DownloadResult>((resolve) => {
                resolve({ status: 'success', message: 'File already downloaded.', code: 200 })
            })
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
            if (this.fileSize() > 0){
                fs.unlinkSync(this.data.path)
            }
            this.data.end_at = Date.now()
            this.data.last_update = Date.now()
            if (error.name === 'AbortError') {
                logger.log('info', `successfully aborted ${this.id()}`)
            } else {
                return new Promise<DownloadResult>((resolve) => {
                    resolve({ status: 'error', message: JSON.stringify(error), code: 500 })
                })
            }
        }
        if (response){
            const { status } = response as any
            if (status !== 200){
                this.data.end_at = Date.now()
                this.data.last_update = Date.now()
                return new Promise<DownloadResult>((resolve) => {
                    resolve({ status: 'error', message: 'Error downloading file.', code: status })
                })
            }
        }
        this.data.total_size = parseInt(response?.headers['content-length']);

        return new Promise<DownloadResult>((resolve) => {
            response?.data.on('data', (chunk: any) => {
                this.data.size_downloaded += chunk.length
                if (Date.now() - this.data.last_update > MIN_INTERVAL_DOWNLOAD_STATUS){
                    this.data.last_update = Date.now();
                    this.printStatus()
                }
            })

            const writer = response?.data.pipe(fs.createWriteStream(this.data.path));
            writer.on('finish', () => {
                this.data.end_at = Date.now();
                this.data.last_update = Date.now();
                resolve({ status: 'success', message: 'File downloaded successfully.', code: 200 })
            });
            writer.on('error', (e) => {
                this.data.end_at = Date.now();
                this.data.last_update = Date.now();
                fs.unlinkSync
                resolve({ status: 'error', message: 'Error writing file:' + JSON.stringify(e), code: 500 })
            });

        })
    }
}


let _interval: NodeJS.Timeout | undefined = undefined
let _i = 0

export class DownloadEngine {

    private _countDownloaded = 0
    private downloads: Download[] = []
    private _pauseUntil = 0
    private _pauseTimeout: NodeJS.Timeout | undefined = undefined

    constructor(){
        _interval = setInterval(this.printStatus, 5_000)
    }

    printStatus = () => {
        const pending = this.downloads.filter(d => d.isBlank()).length
        const downloading = this.downloads.filter(d => d.isDownloading()).length > 0 ? 'yes' : 'no'
        if (pending > 0 || downloading === 'yes'){
            logger.log('info', `Status`, {
                done: this._countDownloaded,
                pending,
                downloading,
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
        this._pauseTimeout = setTimeout(this.run, (seconds+1) * 1000)
    }

    add = (symbol: Symbol, date: string) => {
        const url = symbol.buildURL(date)

        const d = this.downloads.find(d => d.url() === url)
        if (!d){
            const d = new Download(url, symbol.buildArchivePath(date), symbol.setID())
            this.downloads.push(d)
        }
        this.run()
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

    run = () => {        
        if (this._pauseUntil > Date.now()){
            return
        }
        const dlOnes = this.downloads.filter(d => d.isDownloading())
        if (dlOnes.length >= MAX_PARALLEL_DOWNLOAD){
            return
        }

        this.downloads.forEach(d => d.isDownloading() && d.hasNoUpdates() && this.remove(d.url(), true))

        const inactive = this.downloads.find(d => !d.hasStarted())
        if (inactive){
             inactive.start().then((res) => {
                if (res?.code=== 200){
                    this._countDownloaded++
                    //check it's not cached
                    if (inactive.hasDownloaded()){
                        const filename = inactive.url().split('/').pop()
                        logger.log('info', green(`Successfully downloaded ${extractSymbolFromTradeZipFile(filename || '')} archive ${extractDateFromTradeZipFile(filename || '')}`), {
                            time: accurateHumanize(inactive.downloadTime()),
                        })
                    }
                } else if (res?.code === 429){
                    this.pause(60)
                    logger.log('warn', `Rate limited, pausing for 60 seconds`)
                } else if (res?.code === 404){
                    this.remove(inactive.url())
                    logger.log('warn', `File not found ${inactive.url()}`)
                } else {
                    logger.log('error', `Error downloading ${inactive.url()}`, {
                        code: res?.code,
                        message: res?.message
                    })
                    this.remove(inactive.url(), true)
                    this.pause(30)   
                }
             }).catch((e) => {
                logger.log('error', `Unhandled error downloading ${inactive.url()}`, {
                    message: JSON.stringify(e)
                })
                this.remove(inactive.url(), true)
                this.pause(30)
             }).finally(() => {
                    this.run()
             })
        }
    }
}

const downloadEngine = new DownloadEngine()
export default downloadEngine