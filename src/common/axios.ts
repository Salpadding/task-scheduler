import Axios from 'axios'
import http from 'http'
import https from 'https'

export default function axios() {
    const agent = (provider: typeof http | typeof https) => new provider.Agent({
        keepAlive: true,
        maxSockets: parseInt(process.env['AXIOS_MAX_SOCKETS'] || '32'),
        maxFreeSockets: parseInt(process.env['AXIOS_MAX_SOCKETS'] || '8'),
        timeout: parseInt(process.env['AXIOS_TIMEOUT'] || '60000'),
    });

    return Axios.create({
        httpAgent: agent(http),
        httpsAgent: agent(https),
        timeout: parseInt(process.env['AXIOS_TIMEOUT'] || '60000'),
    })
}