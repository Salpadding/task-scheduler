# 回调/任务调度中心

## 功能项

1. 把同步调用变成异步调用 + 排队, 需要阻塞方确认任务结束

```js
const app = express()
app.use(express.json())

// api for receive call back
app.post('/callback', (req, res) => {
    console.log('callback done result = ')
    console.log(req.body)
    res.json({ok: true})
})

// long time api
app.post('/work', (req, res) => {
    console.log('start working payload = ')
    console.log(req.body)
    const callback = req.query['callback']
    res.json({ok: true})

    setTimeout(() => {
        console.log('working done')
        axios.post(callback, {ok: true, data: 'work done'})
    }, 15 * 1000)
})

app.listen(3000, async () => {
    axios.post('http://task-scheduler/task/new', {}, {
        params: {
            callee: 'http://localhost:3000/work',
            callback: 'http://localhost:3000/callback',
            group: 'transfer', // 并发组 同组的任务会排队
        }
    })
})
```

2. 把同步调用变成异步调用 + 排队, 不需要阻塞方确认任务结束

```js
const app = express()
app.use(express.json())

// api for receive call back
app.post('/callback', (req, res) => {
    console.log('callback done result = ')
    console.log(req.body)
    res.json({ok: true})
})

// long time api
app.post('/work', (req, res) => {
    console.log('start working payload = ')
    console.log(req.body)
    const callback = req.query['callback']

    setTimeout(() => {
        console.log('working done')
        res.json({ok: true, data: 'working done'})
    }, 15 * 1000)
})

app.listen(3000, async () => {
    const task = new URL('http://task-scheduler/callback/new')
    task.searchParams.set('callee', 'http://localhost:3000/work')

    axios.post('http://task-scheduler/task/new', {}, {
        params: {
            callee: task.toString(),
            callback: 'http://localhost:3000/callback',
            group: 'transfer', // 并发组 同组的任务会排队
        }
    })
})
```


3. 超时回调

```js
axios.post('http://task-scheduler/callback/new', {}, {
    params: {
        callee: '', // 可选 如果不为空 会把调用 callee 的结果通过接口回给 callback
        callback: 'http://localhost:3000/callback',
        setTimeout: 3000
    }
})
```

4. 定时回调

```js
axios.post('http://task-scheduler/callback/new', {}, {
    params: {
        callee: '', // 可选 如果不为空 会把调用 callee 的结果通过接口回给 callback
        callback: 'http://localhost:3000/callback',
        setInterval: 3000
    }
})
```


5. 防抖回调

```js
axios.post('http://task-scheduler/callback/new', {}, {
    params: {
        callee: '', // 可选 如果不为空 会把调用 callee 的结果通过接口回给 callback
        callback: 'http://localhost:3000/callback',
        key: '', // 相同的 key 在同一个窗口内 只会被回调一次
        window: 8000, // 窗口大小
    }
})
```

6. 异步任务编排

```js
async function testSeqTask() {
    const app = express()
    app.use(express.json())

    app.post('/task/:step', async (req, res) => { // worker 收到调度器的信号开始工作
        console.log(req.params.step) // 打印当前 worker
        let nonce = parseInt(req.body.nonce)
        nonce++
        res.json({ ok: true, poll: { url: '' } }) // +1 然后 return 给调度器 调度器回给下一个 worker, poll 可以用来告知调度器此任务需要轮询
        await axios.post(req.query['callback'], { nonce: nonce }) // 调度器的传过来 callback 参数用来确认任务完成 + 返回值给下一个任务
    })

    app.listen(port, async () => {
        const methods = ['/task/step1', '/task/step2', '/task/step3', '/task/step4', '/task/step5', '/task/step6']
            .map(x => `http://localhost:${port}${x}`)

        let url = '' // 合成 callee callback -> url
        for (let i = methods.length - 1; i >= 0; i--) {
            const u = new URL('http://localhost:3010/task/new')
            if (!url) {
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
```