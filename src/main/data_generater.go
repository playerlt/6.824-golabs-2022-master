package main

import (
	"fmt"
	"io"
	"os"
)

func main() {
	const repeated_times = 100
	paths := os.Args[1:]
	fmt.Println(paths)
	for _, path := range paths {
		file, _ := os.Open(path)
		defer file.Close()
		content, _ := io.ReadAll(file)
		tempfile, _ := os.CreateTemp("./test_data", "test_temp")
		for i := 0; i < repeated_times; i++ {
			_, err := tempfile.Write(content)
			if err != nil {
				fmt.Printf("error write: %v\n", err)
				return
			}
		}
		err := os.Rename(tempfile.Name(), "./test_data/"+path)
		if err != nil {
			fmt.Printf("error rename: %v\n", err)
			return
		}
	}
}
