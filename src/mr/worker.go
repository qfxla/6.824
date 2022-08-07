package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//用于排序
type KeyValues []KeyValue

// for sorting by key.
func (a KeyValues) Len() int           { return len(a) }
func (a KeyValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KeyValues) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//time.Sleep(5 * time.Second)
	// Your worker implementation here.
	for {
		ret, reply := callDistributeTask()
		if !ret {
			log.Println("call master failed, job has been finished")
			break
		}
		//log.Printf("Get %s Task, name:%s, nReduce:%d", reply.TaskType.String(), reply.FileName, reply.NReduce)
		switch reply.TaskType {
		case Unkown:
			time.Sleep(10 * time.Millisecond)
		case Map:
			doMap(reply.FileName, reply.NReduce, mapf)
		case Reduce:
			doReduce(reply.FileName, reply.NReduce, reducef)
		default:
			log.Panicf("invalid taskType %v\n", reply.TaskType)
		}
	}

	// uncomment to send the Example RPC to the master.
	//CallExample()
}

//完成MapTask
//输入文件大小固定可控
func doMap(fileName string, nReduce int, mapf func(string, string) []KeyValue) {

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	//执行用户的Map
	kva := mapf(fileName, string(content))
	//排序
	sort.Sort(KeyValues(kva))
	//生成中间数据
	kvBuckets := make([][]KeyValue, nReduce)
	for i := range kvBuckets {
		kvBuckets[i] = []KeyValue{}
	}
	for _, kv := range kva {
		index := ihash(kv.Key) % nReduce
		kvBuckets[index] = append(kvBuckets[index], kv)
	}
	//生成中间文件
	outputIntermediateFile(path.Base(fileName), kvBuckets)
	//完成任务
	callFinishTask(Map, fileName)
}

//完成ReduceTask
//输入文件内容不可控
func doReduce(fileName string, nReduce int, reducef func(string, []string) string) {
	oname := "mr-out-" + fileName
	intermediateKvs := []KeyValue{}
	inputFiles := getReduceInputFiles(fileName, nReduce)
	for _, inputFile := range inputFiles {
		if inputFile == nil {
			continue
		}
		dec := json.NewDecoder(inputFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediateKvs = append(intermediateKvs, kv)
		}
	}
	sort.Sort(KeyValues(intermediateKvs))

	ofile, _ := ioutil.TempFile("./", oname)

	i := 0
	for i < len(intermediateKvs) {
		j := i + 1
		for j < len(intermediateKvs) && intermediateKvs[j].Key == intermediateKvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediateKvs[k].Value)
		}
		output := reducef(intermediateKvs[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediateKvs[i].Key, output)
		i = j
	}

	ofile.Close()
	os.Rename(ofile.Name(), oname)

	callFinishTask(Reduce, fileName)
}

func getReduceInputFiles(fileName string, nReduce int) []*os.File {
	files := make([]*os.File, nReduce)
	fileInfos, _ := ioutil.ReadDir("./")
	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() {
			name := fileInfo.Name()
			nameParts := strings.Split(name, "-")
			if nameParts[0] == "mr" && nameParts[len(nameParts)-1] == fileName {
				file, _ := os.Open(name)
				files = append(files, file)
			}
		}
	}
	return files
}

func outputIntermediateFile(fileName string, kvBuckets [][]KeyValue) {
	for i, kvBucket := range kvBuckets {
		outputFileName := "mr-" + fileName + "-" + strconv.Itoa(i)
		outputFile, _ := ioutil.TempFile("", outputFileName)
		enc := json.NewEncoder(outputFile)
		for _, kv := range kvBucket {
			enc.Encode(kv)
		}
		outputFile.Close()
		os.Rename(outputFile.Name(), outputFileName)
	}
}

//请求Master分发任务
func callDistributeTask() (bool, *DistrubuteTaskReply) {
	reply := &DistrubuteTaskReply{}
	ret := call("Master.DistributeTask", &DistributeTaskArgs{}, reply)
	if ret {
		return true, reply
	} else {
		return false, reply
	}
}

//请求完成任务
func callFinishTask(taskType TaskType, fileName string) {
	args := &FinishTaskArgs{
		TaskType: taskType,
		FileName: fileName,
	}
	call("Master.FinishTask", args, &FinishTaskReply{})
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}