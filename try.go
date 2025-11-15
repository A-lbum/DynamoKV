package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

var store = make(map[string]float64)

func Put(key string, value float64) {
	store[key] = value
}

func Get(key string) (float64, bool) {
	v, ok := store[key]
	return v, ok
}

func Delete(key string) {
	delete(store, key)
}

func List() {
	if len(store) == 0 {
		fmt.Println("(empty)")
		return
	}
	for k, v := range store {
		fmt.Printf("%s → %.2f\n", k, v)
	}
}

func main() {
	fmt.Println("=== Mini KVStore ===")
	fmt.Println("命令：put key value | get key | delete key | list | exit")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		args := strings.Fields(line)
		if len(args) == 0 {
			continue
		}

		switch args[0] {
		case "put":
			if len(args) != 3 {
				fmt.Println("用法: put key value")
				continue
			}
			val, err := strconv.ParseFloat(args[2], 64)
			if err != nil {
				fmt.Println("错误: value 必须是数字")
				continue
			}
			Put(args[1], val)
			fmt.Println("已保存")

		case "get":
			if len(args) != 2 {
				fmt.Println("用法: get key")
				continue
			}
			if v, ok := Get(args[1]); ok {
				fmt.Printf("%s = %.2f\n", args[1], v)
			} else {
				fmt.Println(" 未找到该 key")
			}

		case "delete":
			if len(args) != 2 {
				fmt.Println("用法: delete key")
				continue
			}
			Delete(args[1])
			fmt.Println("已删除")

		case "list":
			List()

		case "exit":
			fmt.Println("退出程序")
			return

		default:
			fmt.Println("未知命令:", args[0])
		}
	}
}
