package sparse_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
)

func must2[Ret any](r Ret, err error) Ret {
	if err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Println("Terminated")
			os.Exit(1)
		}
		panic(fmt.Sprintf("%+v", err))
	}
	return r
}

func currentCallerFuncShortName(skip int) string {
	name := currentCallerFuncName(skip + 1)
	if name == "" {
		return ""
	}

	return name[strings.LastIndex(name, ".")+1:]
}

// skip = -1 will return currentCallerFuncName, skip = 0 will return the caller of currentCallerFuncName, etc.
func currentCallerFuncName(skip int) string {
	pc, _, _, ok := runtime.Caller(skip + 1)
	if !ok {
		return ""
	}

	details := runtime.FuncForPC(pc)
	if details == nil {
		return ""
	}

	return details.Name()
}

func currentCallerCodeLineNum(skip int) int {
	pc, _, _, ok := runtime.Caller(skip + 1)
	if !ok {
		return 0
	}

	details := runtime.FuncForPC(pc)
	if details == nil {
		return 0
	}

	_, line := details.FileLine(pc)

	return line
}
