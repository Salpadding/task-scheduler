const axios = require('axios').default
const express = require('express')

const port = 7003

async function testCallback() {
    const app = express()
    app.use(express.json())

    app.listen(port, () => {
        console.log(`server listen on ${port}`)

        for (let i = 0; i < 10; i++) {
            const resp = axios.post('http://localhost:3010/callback/new', undefined, {
                params: {
                    callback: `http://localhost:${port}/callback`,
                    setTimeout: 6000,
                }
            })
            resp.catch(() => { })
        }
    })

    app.post('/callback', (req, res) => {
        res.json({ ok: true })
        console.log('callback received')
    })

}


async function testInterval() {
    const app = express()
    app.use(express.json())
    let i = 0
    app.listen(port, () => {
        const resp = axios.post('http://localhost:3010/callback/new', {}, {
            params: {
                callback: `http://localhost:${port}/callback`,
                setInterval: 1000,
            }
        })
        resp.catch(() => { })
    })

    app.post('/callback', (req, res) => {
        i++
        console.log('callback received')
        if (i < 10) {

            res.json({ ok: true })
            return
        }

        console.log('completed')
        res.json({ ok: true, data: true })
    })

}

async function testTask() {
    const app = express()
    app.use(express.json())

    const completed = []

    app.get('/task/check/:id', async (req, res) => {
        const id = parseInt(req.params.id)
        if (completed[id]) {
            res.json({ ok: true, data: id })
            return
        }
        res.status(404).end()
        return
    })

    app.post('/callback/task/new', (req, res) => {
        res.json({
            ok: true, data: {
                poll: {
                    url: `http://localhost:${port}/task/check/${req.body.tid}`,
                    interval: 18 * 1000,
                }
            }
        })
        console.log(`start execute task ${req.body.tid}`)
        const confirm = req.query['callback']

        setTimeout(async () => {
            console.log('complete task ' + req.body.tid)
            console.log(`confirm by post ${confirm}`)
            completed[parseInt(req.body.tid)] = true
            // await axios.post(confirm, { ok: true, data: req.body.tid })
        }, 3 * 1000)
    })


    app.post('/callback/task/end', (req, res) => {
        res.json({ ok: true })
        console.log('receive task end callback')
        console.log(req.body)
    })

    app.listen(port, async () => {
        console.log(`server listen on ${port}`)

        for (let i = 0; i < 10; i++) {
            const tid = i
            const resp = await axios.post('http://localhost:3010/task/new', {
                tid: tid,
            }, {
                params: {
                    group: 'transfer',
                    callee: `http://localhost:${port}/callback/task/new`,
                    callback: `http://localhost:${port}/callback/task/end`,
                }
            })
            console.log(`create task`)
            console.log(resp.data)
        }
    })


}

async function testSeqTask() {
    const app = express()
    app.use(express.json())

    app.post('/task/:step', async (req, res) => { // worker 收到调度器的信号开始工作
        console.log(req.params.step) // 打印当前 worker
        let nonce = parseInt(req.body.nonce)
        nonce++
        res.json({ ok: true, poll: {url: ''} }) // +1 然后 return 给调度器 调度器回给下一个 worker, poll 可以用来告知调度器此任务需要轮询
        await axios.post(req.query['callback'], { nonce: nonce }) // 调度器的传过来 callback 参数用来确认任务完成 + 返回值给下一个任务
    })

    app.listen(port, async () => {
        const methods = [ '/task/step1','/task/step2','/task/step3','/task/step4','/task/step5','/task/step6']
            .map(x => `http://localhost:${port}${x}`)

        let url = '' // 合成 callee callback -> url
        for (let i = methods.length - 1; i >= 0; i--) {
            const u = new URL('http://localhost:3010/task/new')
            if(!url) {
                u.searchParams.set('callee', methods[i])
                url = u.toString()
                continue
            }
            u.searchParams.set('callee', methods[i]) // callee 是 worker 收到信号的任务
            u.searchParams.set('callback', url) // 调度器确认worker 完成任务后会执行 callback 回调 
            // 这里把 /task/new 作为回调 这样上一个任务完成后自动执行下一个任务
            url = u.toString()
        }

        await axios.post(url, { nonce: 0 })
    })

}

testSeqTask().catch(console.error)


