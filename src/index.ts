import e, { Express, Request, Response } from 'express'
import express from 'express'
import redis from 'redis'
import fs from 'fs'
import amqp from 'amqplib'
import * as Axios from './common/axios.js'
import { v4 as uuidv4 } from 'uuid'

const axios = Axios.default()

export const Topics = {
    callBackCreate: 'callback.create',
    taskCreate: 'task.create',
    taskEnd: 'task.end',
    taskTimeout: 'task.timeout',
}


export interface TaskEntity {
    id: string
    group: string
    parallels?: number
    callee: {
        url: string,
        payload: any
    }
    callback?: {
        url: string
        payload?: any
    }
}

export class BaseServer {
    redisCli = redis.createClient({ url: process.env['REDIS_URL'] })
    app = express()
    channel: amqp.Channel

    redisLuaScripts = {
        'redis_task_push': '',
        'redis_task_end': '',
    }

    params = {
        redisPrefix: process.env['REDIS_RESOURCE_PREFIX'] || 'task-scheduler',
        redisResourceTtl: parseInt(process.env['REDIS_RESOURCE_TTL'] || (7 * 24 * 3600).toString()),
        rabbitmqPrefix: process.env['RABBITMQ_RESOURCE_PREFIX'] || 'task-scheduler',
        rescheduleAfter: parseInt(process.env['APP_RESCHEDULE_AFTER'] || (60 * 1000).toString()),
        taskTimeout: parseInt(process.env['APP_TASK_TIMEOUT'] || (6 * 3600 * 1000).toString()),
        taskPollInterval: parseInt(process.env['APP_TASK_POLL_INTERVAL'] || (8 * 1000).toString()),
        self: process.env['SELF'] || 'http://task-scheduler'
    }

    constructor() {
    }

    async init() {
        const rabbitmq = await amqp.connect(process.env['RABBITMQ_URL'])
        this.channel = await rabbitmq.createConfirmChannel()

        this.app.use(express.json())
        await this.redisCli.connect()



        // create lua script
        for (const key of Object.keys(this.redisLuaScripts)) {
            const script = fs.readFileSync(`${key}.lua`, 'utf8')
            this.redisLuaScripts[key] = await this.redisCli.scriptLoad(script)
        }

        const ch = this.channel
        const rabbitmqPrefix = this.params.rabbitmqPrefix

        await ch.assertExchange(`${rabbitmqPrefix}`, 'topic', { durable: true })

        // create callback queue
        const callbackQueue = `${rabbitmqPrefix}-callback-consumer`
        await ch.assertQueue(callbackQueue, { durable: true })
        await ch.bindQueue(callbackQueue, `${rabbitmqPrefix}`, Topics.callBackCreate)

        // task.schedule task.create task.end
        // create task queue
        const taskScheduleQueue = `${rabbitmqPrefix}-task-consumer`
        await ch.assertQueue(taskScheduleQueue, { durable: true })

        // wait for task.end events
        await ch.bindQueue(taskScheduleQueue, rabbitmqPrefix, 'task.*')

        // handle callback
        ch.consume(callbackQueue, (msg) => this.onCallbackMsg(msg), {
            noAck: false
        })

        ch.consume(taskScheduleQueue, (msg) => this.onTaskMsg(msg), {
            noAck: false
        })

        // http -> rabbitmq
        this.app.post('/publish', async (req, res) => {
            const body: { exchange: string, routingKey: string, payload: any } = req.body
            console.log(`PUBLISH to exchange ${body.exchange} routingKey ${body.routingKey} payload ${JSON.stringify(body.payload)}`)
            this.channel.publish(body.exchange, body.routingKey, Buffer.from(JSON.stringify(body.payload), 'utf8'))
            res.json({ ok: true })
        })
    }

    async onTaskMsg(msg: amqp.Message) { }

    async onCallbackMsg(msg: amqp.Message) {
        try {
            const opts: {
                callback: string
                body: any
                window?: number
                key?: string
                id?: string
            } = JSON.parse(msg.content.toString('utf8'))

            console.log(`CALLBACK ${opts.callback} with payload ${JSON.stringify(opts.body)}`)
            const resp = await axios.post(opts.callback, opts.body, { timeout: this.params.rescheduleAfter })
            console.log(`CALLBACK ${opts.callback} with payload ${JSON.stringify(opts.body)} status code = ${resp.status}`)
            if (resp.status >= 400) {
                throw new Error(`callback failed`)
            }


        } catch (e) {
            console.error(e)
            // retry
            await axios.post(`${this.params.self}/callback/new`, {
                exchange: msg.fields.exchange,
                routingKey: msg.fields.routingKey,
                payload: JSON.parse(msg.content.toString('utf8'))
            }, {
                params: {
                    setTimeout: this.params.rescheduleAfter,
                    callback: `${this.params.self}/publish`
                }
            })
        } finally {
            this.channel.ack(msg)
        }
    }
}

export interface CallbackQuery {
    setTimeout?: number
    setInterval?: number
    callback: string
    window?: number
    key?: string
}

export interface CallbackMQPayload {
    callback: string
    body: any
    id?: string
}

export class CallbackServer extends BaseServer {
    supportedWindows = [1, 2, 4, 8, 16, 32, 64].map(x => x * 1000)

    async onWindowCallback(req: Request, res: Response, cb: CallbackQuery, p: CallbackMQPayload) {
        if (!this.supportedWindows.find(x => cb.window)) {
            res.json({
                ok: false,
                data: 'unsupported window value'
            })
            return
        }
        await this.redisCli.setEx(`${this.params.redisPrefix}::callback::${cb.key}`, this.supportedWindows[this.supportedWindows.length - 1] / 1000 * 16, JSON.stringify(p))
        await this.redisCli.sAdd(`${this.params.redisPrefix}::window::${cb.window}`, cb.key)
        res.json({
            ok: true
        })

    }


    async init() {
        await super.init()

        // supported windows
        for (const w of this.supportedWindows) {
            // watch redis keys
            const timer = async () => {
                const values = await this.redisCli.sPop(`${this.params.redisPrefix}::window::${w}`, 1 << 30)
                for (const v of values) {
                    const payload = await this.redisCli.get(`${this.params.redisPrefix}::callback::${v}`)
                    if (!payload) continue
                    this.channel.publish(`${this.params.rabbitmqPrefix}`, Topics.callBackCreate, Buffer.from(payload, 'utf8'))
                }
                setTimeout(timer, w)
            }
            timer()
        }

        const setTimeoutSet = `${this.params.redisPrefix}::setTimeout::set`
        const dataKeyOf = (id: string) => `${this.params.redisPrefix}::setTimeout::data::${id}`

        const timer = async () => {
            const now = Date.now()
            const values = await this.redisCli.zRangeByScore(setTimeoutSet, 0, now)
            for (const v of values) {
                const dataKey = dataKeyOf(v)
                const payload = await this.redisCli.get(dataKey)
                if (!payload) continue
                if (!(await this.redisCli.zRem(setTimeoutSet, v))) {
                    continue
                }
                this.channel.publish(`${this.params.rabbitmqPrefix}`, Topics.callBackCreate, Buffer.from(payload, 'utf8'))
                await this.redisCli.del(dataKey)
            }
            setTimeout(timer, 1000)
        }

        timer()

        // 定时回调 上游服务 return {data: true} 结束定时回调
        // callback = 上游 
        // setTimeout = 定时回调间隔
        this.app.post('/private/callback/interval', async (req, res) => {
            const backend = req.query['callback'] as string
            const setTimeout = parseInt(req.query['setTimeout'] as string)

            if (!backend || !setTimeout || isNaN(setTimeout)) {
                res.json({ ok: true })
                return
            }

            try {
                const resp = await axios.post(backend, req.body)
                if (resp.data?.data) {
                    res.json({ ok: true })
                    return
                }
            } catch (e) {
                console.error(e)
                res.status(500).json({ ok: false })
                return
            }

            await axios.post(`${this.params.self}/callback/new`, req.body, {
                params: {
                    callback: `${this.params.self}${req.url}`,
                    setTimeout: setTimeout
                }
            })
            res.json({ ok: true })
        })


        // callback server
        // 1. supports 定时回调
        // 2. supports 防抖
        this.app.post('/callback/new', async (req, res) => {
            const callbackQuery: CallbackQuery = {} as any

            const queryParams = [
                ['setTimeout', parseInt],
                ['setInterval', parseInt],
                'callback', ['window', parseInt],
                'key',
            ]

            for (const p of queryParams) {
                if (typeof p === 'string') {
                    (typeof req.query[p] === 'string') && (callbackQuery[p] = req.query[p])
                    continue
                }
                if (Array.isArray(p)) {
                    if (typeof req.query[p[0] as string] !== 'string') continue
                    const fn = p[1] as any
                    callbackQuery[p[0] as string] = fn(req.query[p[0] as string])
                }

            }


            if (typeof callbackQuery.callback !== 'string') {
                console.error(`invalid callback ${callbackQuery.callback}`)
                res.json({ ok: false })
                return
            }

            const rabbitMqPayload: CallbackMQPayload = {
                callback: callbackQuery.callback,
                body: req.body
            }

            // sliding window callback
            if (callbackQuery.window && callbackQuery.key && !isNaN(callbackQuery.window)) {
                console.log(`WINDOW callback window = ${callbackQuery.window} callback = ${callbackQuery.callback}`)
                this.onWindowCallback(req, res, callbackQuery, rabbitMqPayload)
                return
            }

            if (callbackQuery.setInterval) {
                const u = new URL(`${this.params.self}/private/callback/interval`)
                u.searchParams.set('setTimeout', callbackQuery.setInterval.toString())
                u.searchParams.set('callback', callbackQuery.callback)

                await axios.post(`${this.params.self}/callback/new`, req.body, {
                    params: {
                        callback: u.toString(),
                        setTimeout: callbackQuery.setTimeout
                    }
                })

                res.json({ ok: true })
                return
            }

            // callback immediate or callback with setTimeout
            if (callbackQuery.setTimeout) {
                const callbackId = uuidv4()
                const score = Date.now() + callbackQuery.setTimeout
                console.log(`SET TIMEOUT callback ${callbackQuery.callback} with timeout ${callbackQuery.setTimeout}`)
                await this.redisCli.setEx(dataKeyOf(callbackId), this.params.redisResourceTtl, JSON.stringify(rabbitMqPayload))
                await this.redisCli.zAdd(setTimeoutSet, { score: score, value: callbackId })
                res.json({ ok: true })
                return
            }

            const topic = Topics.callBackCreate

            if (callbackQuery.setTimeout) {
                setTimeout(() => {
                    this.channel.publish(this.params.rabbitmqPrefix, topic, Buffer.from(JSON.stringify(rabbitMqPayload), 'utf8'))
                }, callbackQuery.setTimeout)
                res.json({ ok: true })
                return
            }

            const vars: { ex: string, opts?: amqp.Options.Publish } = { ex: `${this.params.rabbitmqPrefix}` }
            this.channel.publish(vars.ex, topic, Buffer.from(JSON.stringify(rabbitMqPayload), 'utf8'), vars.opts)
            res.json({ ok: true })
        })


    }
}


export class TaskScheduleServer extends CallbackServer {
    groupParallels: Record<string, number> = {}

    async onTaskMsg(msg: amqp.Message): Promise<void> {
        try {
            switch (msg.fields.routingKey) {
                case 'task.create': {
                    await this.handleTaskCreate(msg)
                    break
                }
                case 'task.timeout':
                case 'task.end': {
                    await this.handleTaskEnd(msg)
                    break
                }
                default: {
                    console.log(`unsupported key ${msg.fields.routingKey}`)
                }
            }
        } finally {
            this.channel.ack(msg)
        }
    }

    endTask = async (id: string, ret: any) => {
        const task = await this.getTask(id)
        if (!task) return
        if (task.callback && task.callback.url) task.callback.payload = ret
        await this.saveTask(task)
        this.channel.publish(this.params.rabbitmqPrefix, Topics.taskEnd, Buffer.from(JSON.stringify(task), 'utf8'))
    }

    async init() {
        await super.init()

        const taskGroups = process.env['APP_TASK_GROUPS']

        if (taskGroups)
            for (const pair of taskGroups.split(',')) {
                const [name, limit] = pair.split(':')
                this.groupParallels[name] = parseInt(limit)
            }

        this.app.post('/private/checkTask/:id', async (req, res) => {
            const poll: { url: string, method?: 'get' | 'post' } = req.body
            const id = req.params.id
            console.log(`CEHCK task ${id}`)

            if (!id || !poll?.url) {
                res.json({ ok: true, data: true })
                return
            }

            const task = await this.getTask(id)
            if (!task) {
                res.json({ ok: true, data: true })
                return
            }

            try {
                const resp = await axios[poll.method || 'get'](poll.url, {
                    validateStatus: () => true,
                })
                if (resp && resp.status < 400) {
                    console.log(`CHECK task ${id} ok data = `)
                    console.log(resp.data)
                    await this.endTask(id, resp.data)
                    res.json({ ok: true, data: true })
                    return
                }
            } catch (e) {
                console.error(e)
            }

            res.json({ ok: true, data: false })
        })

        this.app.post('/task/end/:id', async (req, res) => {
            const id = req.params.id
            res.json({ ok: true })

            if (!id) return
            console.log(`END task ${id} from ${req.url} data = `)
            console.log(req.body)
            await this.endTask(id, req.body)
        })

        this.app.post('/task/new', async (req, res) => {
            const task: TaskEntity = {
                id: uuidv4(),
                group: req.query.group as string,
                callee: { url: req.query.callee as string, payload: req.body },
                parallels: req.query.parallels ? parseInt(req.query.parallels as string) : 0,
            }
            if (!task.parallels || isNaN(task.parallels)) delete task['parallels']
            if (req.query.callback) {
                task.callback = { url: req.query.callback as string }
            }
            this.channel.publish(this.params.rabbitmqPrefix, Topics.taskCreate, Buffer.from(JSON.stringify(task), 'utf8'))
            res.json({ ok: true, data: task.id })
        })


        this.app.post('/private/callback/schedule/:id', async (req, res) => {
            const id = req.params.id
            const task = await this.getTask(id)

            if (!task) {
                res.json({ ok: true })
                return
            }
            const u = new URL(task.callee.url)

            u.searchParams.set('id', id)
            u.searchParams.set('callback', `${this.params.self}/task/end/${id}`)
            const resp = await axios.post(u.toString(), task.callee.payload)

            const workerReply: { poll?: { url: string, method: string, interval?: number } } = resp.data?.data
            if (workerReply?.poll?.url) {
                await axios.post(`${this.params.self}/callback/new`, workerReply.poll, {
                    params: {
                        callback: `${this.params.self}/private/checkTask/${id}`,
                        setInterval: workerReply?.poll?.interval || this.params.taskPollInterval,
                        setTimeout: workerReply?.poll?.interval || this.params.taskPollInterval,
                    }
                })
            }

            res.json({ ok: true })
        })

        const timer = async () => {
            for (const key of Object.keys(this.groupParallels)) {
                const mayBeSchedule = await this.redisCli.evalSha(this.redisLuaScripts.redis_task_push, this.keyArgs('', key, this.groupParallels[key])) as string[]

                for (const toSchedule of mayBeSchedule) {
                    const task = await this.getTask(toSchedule)
                    if (task)
                        await this.scheduleTask(task)
                }
            }
            setTimeout(timer, 8 * 1000)
        }

        timer()
    }



    keyArgs = (taskId: string, group: string, parallels: number) => ({
        keys: [
            `${this.params.redisPrefix}::running::set::${group}`,
            `${this.params.redisPrefix}::waiting::set::${group}`,
            `${this.params.redisPrefix}::waiting::list::${group}`,
        ],
        arguments: [parallels.toString(), taskId]
    })

    taskBodyKey = (id: string) => `${this.params.redisPrefix}::task::body::${id}`

    async saveTask(task: TaskEntity) {
        await this.redisCli.setEx(this.taskBodyKey(task.id), this.params.taskTimeout, JSON.stringify(task))
    }

    getTask = async (id: string) => {
        const resp = await this.redisCli.get(this.taskBodyKey(id))
        if (!resp) return
        return JSON.parse(resp) as TaskEntity
    }


    // schedule task as running
    scheduleTask = async (task: TaskEntity) => {
        if (!task) return
        console.log(`SCHEDULE task ${task.id}`)

        this.channel.publish(
            this.params.rabbitmqPrefix, Topics.callBackCreate,
            Buffer.from(JSON.stringify({
                callback: `${this.params.self}/private/callback/schedule/${task.id}`,
            } as CallbackMQPayload), 'utf8')
        )
    }


    // timeout or end
    handleTaskEnd = async (msg: amqp.Message) => {
        if (msg.fields.routingKey !== 'task.end' && msg.fields.routingKey !== 'task.timeout') return
        let task: TaskEntity = JSON.parse(msg.content.toString('utf8'))
        console.log(`END task ${JSON.stringify(task)} from mq =`)
        console.log(task)
        const ok = await this.redisCli.del(this.taskBodyKey(task.id))

        if (ok && task.callback && task.callback.url) {
            this.channel.publish(this.params.rabbitmqPrefix, Topics.callBackCreate, Buffer.from(JSON.stringify(
                {
                    callback: task.callback.url,
                    body: task.callback.payload,
                } as CallbackMQPayload
            ), 'utf8'))
        }

        const mayBeSchedule = (await this.redisCli.evalSha(
            this.redisLuaScripts.redis_task_end,
            this.keyArgs(task.id, task.group, this.groupParallels[task.group])
        )) as string[]

        if (!Array.isArray(mayBeSchedule)) return
        for (const toSchedule of mayBeSchedule) {
            await this.scheduleTask(await this.getTask(toSchedule))
        }
    }

    // handle incoming task
    handleTaskCreate = async (msg: amqp.Message) => {
        if (msg.fields.routingKey !== 'task.create') return
        const task: TaskEntity = JSON.parse(msg.content.toString('utf8'))

        console.log(`CREATE task ${task.id} group = ${task.group}`)
        console.log(task)
        task.group = task.group || '%'
        task.parallels = task.parallels || this.groupParallels[task.group]
        await this.saveTask(task)

        const keyArgs = this.keyArgs(task.id, task.group, task.parallels)
        // maybe new task to schedule
        const mayBeSchedule = (await this.redisCli.evalSha(this.redisLuaScripts.redis_task_push, keyArgs)) as string[]

        if (Array.isArray(mayBeSchedule)) {
            for (const toSchedule of mayBeSchedule) {
                await this.scheduleTask(await this.getTask(toSchedule))
            }
        }

        // 一小时后超时
        await axios.post(`${this.params.self}/callback/new`, {
            exchange: this.params.rabbitmqPrefix,
            routingKey: Topics.taskTimeout,
            payload: task
        }, {
            params: {
                setTimeout: this.params.taskTimeout,
                callback: `${this.params.self}/publish`,
            }
        })
    }
}


async function main() {
    const server = new TaskScheduleServer()
    await server.init()
    const port = process.env['PORT'] || '3000'

    server.app.listen(
        parseInt(port),
        () => console.log(`server listen on port ${port}`)
    )
}

process.on('unhandledRejection', console.error)
main().catch(console.error)
