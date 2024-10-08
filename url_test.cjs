const methods = [
    '/task/step1',
    '/task/step2',
    '/task/step3',
    '/task/step4',
    '/task/step5',
    '/task/step6',
]
    .map(x => `http://task-consumer/${x}`)

let url = ''

for (let i = methods.length - 1; i; i--) {
    if (!url) {
        url = methods[i]
        continue
    }

    const u = new URL('http://task-scheduler/task/new')
    // worker 从这个链接接收回调
    u.searchParams.set('callee', methods[i])
    // worker 结束
    u.searchParams.set('callback', url)
    url = u.toString()
}

console.log(url)