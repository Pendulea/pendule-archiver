import WebSocket from 'ws';
import { logger } from './utils';

const PARSER_SERVER_PORT = parseInt(process.env.PARSER_SERVER_PORT || '8889')

if (isNaN(PARSER_SERVER_PORT) || PARSER_SERVER_PORT < 0 || PARSER_SERVER_PORT > 65535){
    logger.error('Invalid port for parser server')
    process.exit(1)
}

const url = `ws://localhost:${PARSER_SERVER_PORT}`;
const WS_RECONNECT_INTERVAL = 2000;

interface IRequest {
    id     : string;
    method : string;
    payload: {[key: string]: any}
}

interface IResponse {
    id: string;
    data: {[key: string]: any}
    error: string
}

class Service {
    
    private _service: WebSocket;
    private _reconnectTimeout: NodeJS.Timeout | null = null;
    private _requests: Map<string, [(value: {[key: string]: any} | PromiseLike<{[key: string]: any}>) => void, (reason?: any) => void]> = new Map();
    private _reconnect = true
    private _connected = false

    constructor(){
        this.connect()
    }

    waitForConnected = async () => {
        while (!this._connected){
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    }

    request = async (type: string, payload: {[key: string]: any}) => {
        await this.waitForConnected()
        const id = Math.random().toString(10)
        const promise = new Promise<{[key: string]: any}>((resolve, reject) => {
            this._requests.set(id, [resolve, reject]);
        });

        const request: IRequest = {
            id: id,
            method: type,
            payload
        }
        this._service.send(JSON.stringify(request));
        return promise;
    }

    private _handleResponse = (message: string) => {
        try {
            const json = JSON.parse(message) as IResponse;
            const [resolve, reject] = this._requests.get(json.id) || [];
            if (!resolve || !reject) {
                return;
            }
            if (json.error) {
                reject(json.error);
            } else {
                resolve(json.data);
            }
            this._requests.delete(json.id);
        } catch (error) {
            return;
        }
    }

    stop = () => {
        this._reconnect = false;
        if (this._service) {
            this._service.close();
        }
        this._service = null;
        clearTimeout(this._reconnectTimeout || undefined);
    }

    connect = () => {
        if (this._service){
            return
        }
        this._service = new WebSocket(url, {
            headers: {
                'Connection': 'Upgrade',
                'Upgrade': 'websocket'
            }
        });
    
        this._service.onopen = () => {
            logger.log('info', `Connexion with parser is open`, {
                port: PARSER_SERVER_PORT
            })
            this._connected = true;
        };
    
        this._service.onerror = (error) => {
            // logger.log('error', `Error with parser connexion`, {
            //     port: PARSER_SERVER_PORT,
            //     error: JSON.stringify(error)
            // })
        };
    
        this._service.onmessage = (e: WebSocket.MessageEvent) => {
            this._handleResponse(e.data.toString());
        };
    
        this._service.onclose = () => {
            this._connected = false;
            logger.log('warn', `Connexion with parser is closed`, {
                port: PARSER_SERVER_PORT
            })
            this._service = null;
            if (this._reconnect){
                this._reconnectTimeout = setTimeout(this.connect, WS_RECONNECT_INTERVAL);
            }
        };
    }    
}

export const service = new Service()