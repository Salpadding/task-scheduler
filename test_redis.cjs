const redis = require('redis')
const fs = require('fs')

async function main() {
    const client = redis.createClient('redis://localhost:6379')
    await client.connect()

    const file = fs.readFileSync('./redis_task_push.lua', 'utf8')

    const resp = await client.eval(file, {
        keys: ['running::set', 'waiting::set', 'waiting::list'],
        arguments: ['3', 'task::7']
    })
    console.log(resp)
}

main().catch(console.error)
