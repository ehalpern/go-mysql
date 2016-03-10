package main

import (
	"os"
	"bufio"
	"fmt"
"github.com/siddontang/go/log"
	"io"
	"regexp"
)

func main() {
	infile  := "./badread.txt"

	r, out := io.Pipe()
	go saveStream(r)
	in, err := os.Open(infile);
	if err != nil {
		panic(err)
	}
	defer in.Close()
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 1024 * 1024), 1024 * 1024)
	for scanner.Scan() {
		line := scanner.Text()
		line = fmt.Sprintf("%s\n", line)
		log.Infof("Writing to pipe: %v", line)
		_, err := fmt.Fprintf(out, "%v\n", line)
		if err != nil {
			panic(err)
		}
	}
	if err := scanner.Err(); err != nil {
		panic(scanner.Err())
	}
}

func saveStream(in io.Reader) {
	outfile := "./badreadout.txt"
	out, err := os.Create(outfile)
	defer out.Close()
	if err != nil {
		panic(err)
	}
	valuesExp := regexp.MustCompile("^\\((.+)\\)[;,]")

	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 1024 * 1024), 1024 * 1024)
	for scanner.Scan() {
		line := scanner.Text()
		if m := valuesExp.FindStringSubmatch(line); len(m) == 2 {
			log.Debugf("Parse insert line: %s", m[1])
			l := fmt.Sprintf("%s\n", m[1])
			log.Infof("Writing to file %v", l)
			_, err := fmt.Fprintf(out, "%v\n", l)
			if err != nil {
				panic(err)
			}
		}
	}
	out.Sync()
}
