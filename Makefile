include ../include.mk
-include local.mk

all:
	@$(npm_path); tsc

dev:
	@$(npm_path); tsc; node $(node_args) dist/index.js


redis_test:
	@$(foreach x,1 2 3 4 5 6 7,redis-cli -h localhost -n 0 --eval redis_task_push.lua running waiting::list waiting::set \, 3 task::$(x); )


clean:
	@redis-cli flushall
