package main

import (
	"context"
	"io"
	"os"
	"time"

	"gitlab.ozon.ru/platform/tracer-go/logger"
	"gitlab.ozon.ru/validator/broadcaster"
	"gitlab.ozon.ru/validator/goexel"
	"gitlab.ozon.ru/validator/jobs"
	"gitlab.ozon.ru/validator/platform"
)

func main() {
	ctx := context.Background()

	plat := platform.NewPlatform(2, time.Minute, platform.JobPool{
		JobMap: make(map[platform.JobID]platform.Job),
	})
	// доделать
	skuChecker := &jobs.SkuChecker{
		JobWrapper: platform.NewJobWrapper(broadcaster.NewBroadcaster[platform.JobResult](1000)),
		Exists: map[int64]struct{}{
			326585538:  {},
			327110952:  {},
			1020030897: {},
			783714036:  {},
			608775475:  {},
			425637863:  {},
			505028007:  {},
		}}
	plat.AddJob(skuChecker)
	skuValidator := &jobs.IsSkuValid{
		JobWrapper: platform.NewJobWrapper(broadcaster.NewBroadcaster[platform.JobResult](1000)),
	}
	plat.AddJob(skuValidator)

	f, err := os.Open("/Users/pshlykov/Downloads/9ffda052-bfb5-409f-9b3c-b00f447d9400.xlsx")
	if err != nil {
		logger.Fatal(ctx, "failed to open file: %v", err)
	}
	bytes, _ := io.ReadAll(f)

	ff, err := goexel.NewFile[jobs.Entry](bytes)
	if err != nil {
		logger.Fatal(ctx, "failed to decode file: %v", err)
	}
	ff.CellRegister.SetSheet("Sheet1")

	ctx = goexel.SetFileContext(ctx, ff)
	plat.Run(ctx, []platform.JobID{skuChecker.GetID(), skuValidator.GetID()})

	newF := ff.CellRegister.GetFileBytes()
	os.WriteFile("/Users/pshlykov/Downloads/testing.xlsx", newF, 0666)
}
