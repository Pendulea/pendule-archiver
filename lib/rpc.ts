import WebSocket from 'ws';

const url = 'ws://localhost:8080';
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

    request = async (type: string, payload: any) => {
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
            console.log('Websocket with Pendule Parser is open')
            this._connected = true;
        };
    
        this._service.onerror = (error) => {
            // console.log(`Connection Error: ${error.toString()}`);
        };
    
        this._service.onmessage = (e: WebSocket.MessageEvent) => {
            this._handleResponse(e.data.toString());
        };
    
        this._service.onclose = () => {
            this._connected = false;
            console.log('Websocket with Pendule Parser is closed')
            this._service = null;
            if (this._reconnect){
                this._reconnectTimeout = setTimeout(this.connect, WS_RECONNECT_INTERVAL);
            }
        };
    }    
}

export const service = new Service()