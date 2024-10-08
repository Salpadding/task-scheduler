export class AsyncLock {
    data: Record<string, Promise<any>>
    constructor() {
        this.data = {}
    }

    async lock(key: string): Promise<{ resolveFn: any, done: Promise<any> }> {
        const prevDone = this.data[key]
        let resolveFn: any

        const done = new Promise((resolve, reject) => {
            resolveFn = resolve
        })

        this.data[key] = done

        if (prevDone && prevDone.then)
            await prevDone

        return {
            resolveFn: resolveFn,
            done: done
        }
    }

    unlock(key: string, ctx: { resolveFn: any, done: Promise<any> }) {
        if (this.data[key] === ctx.done) {
            delete this.data[key]
        }
        ctx.resolveFn()
    }
}

