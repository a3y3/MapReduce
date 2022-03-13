# MapReduce
This is an implementation of a highly scalable, fault tolerant and performant map reduce library.

- Highly scalable: No hard limit on the number of "worker" nodes. Adding new worker nodes will scale the processing linerarly.
- Fault tolerant: Support for detecting if worker nodes have gone down, then assigning any incomplete tasks to other workers.
- Performant: Support for detecting straggler nodes, then assigning their tasks to other nodes.

## Run
In the directory `src/main` run:
```
go run -race mrcoordinator.go data/pg-*
```

In another window, cd to the same directory and run:
```
go build -race -buildmode=plugin ../mrapps/wc.go && go run -race mrworker.go wc.so
```

This will produce an output of N files with the format `mr-out-X`. To view the complete output, run:
`cat mr-out-* | sort | more`.

## Test
Run `test-mr.sh`.
