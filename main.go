package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"gitlab.ozon.ru/platform/tracer-go/logger"
	"gitlab.ozon.ru/validator/broadcaster"
	"gitlab.ozon.ru/validator/goexel"
	"gitlab.ozon.ru/validator/jobs"
	"gitlab.ozon.ru/validator/platform"
)

var boundedStrLayout = "-----------------------\n%s\n--------------------------------\n"

func main() {
	log.Default().SetFlags(log.Ltime)
	if len(os.Args) < 2 {
		log.Fatalf("usage: %s ", color.HiMagentaString("/path/to/file.xlsx"))
	}
	os.Args = append(os.Args, "--local-config-enabled")

	filepath := os.Args[1]
	//nolint:gosec
	validationFile, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("failed to open %s: %s", filepath, color.RedString(err.Error()))
	}
	log.Printf(boundedStrLayout, color.YellowString("start app initialization"))
	ctx := context.Background()

	plat := platform.NewPlatform(time.Minute, platform.JobPool{
		JobMap: make(map[platform.JobID]platform.Job),
	})
	// доделать
	skuChecker := &jobs.SkuChecker{
		JobWrapper: &platform.JobWrapper{ResChan: &broadcaster.Broadcaster[platform.JobResult]{}},
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
		JobWrapper: &platform.JobWrapper{ResChan: &broadcaster.Broadcaster[platform.JobResult]{}},
	}
	plat.AddJob(skuValidator)

	dataValidator := &jobs.DataValidation{
		JobWrapper: &platform.JobWrapper{ResChan: &broadcaster.Broadcaster[platform.JobResult]{}},
	}
	plat.AddJob(dataValidator)

	funValidator := &jobs.FunValidation{
		JobWrapper: &platform.JobWrapper{ResChan: &broadcaster.Broadcaster[platform.JobResult]{}},
	}
	plat.AddJob(funValidator)

	sorting := &jobs.Sorting{
		JobWrapper: &platform.JobWrapper{ResChan: &broadcaster.Broadcaster[platform.JobResult]{}},
	}
	plat.AddJob(sorting)

	batchVolumeValidation := &jobs.BatchVolumeValidation{
		JobWrapper: &platform.JobWrapper{ResChan: &broadcaster.Broadcaster[platform.JobResult]{}},
		PrivilegedClusters: map[string]struct{}{
			"Москва и область":          {},
			"Санкт-Петербург и область": {},
		},
	}
	plat.AddJob(batchVolumeValidation)

	clusterValidation := &jobs.IsClusterValid{
		JobWrapper: &platform.JobWrapper{ResChan: &broadcaster.Broadcaster[platform.JobResult]{}},
		ValidClusters: map[string]struct{}{
			"ФФ БО":            {},
			"Федеральный":      {},
			"Тверь":            {},
			"Москва и область": {},
			"Набережные Челны": {},
			"Казань":           {},
			"Краснодар":        {},
			"Волгоград":        {},
			"Сочи":             {},
			"Ростов":           {},
			"Санкт-Петербург и область": {},
		},
	}
	plat.AddJob(clusterValidation)

	start := time.Now()

	bytes, _ := io.ReadAll(validationFile)
	ff, err := goexel.NewFile[jobs.Entry](bytes)
	if err != nil {
		logger.Fatal(ctx, "failed to decode file: %v", err)
	}
	ff.CellRegister.SetSheet(ff.Table[0].PromoName.GetSheetName())

	ctx = goexel.SetFileContext(ctx, ff)
	err = plat.Run(ctx, []platform.JobID{funValidator.GetID(), skuChecker.GetID(), batchVolumeValidation.GetID()})
	if err != nil {
		log.Fatalf(color.RedString("failed to validate file: ") + err.Error())
	}

	timeElapsed := time.Since(start).Seconds()
	timeStr := color.GreenString("%f", timeElapsed)
	if timeElapsed > 120 {
		timeStr = color.RedString("%f", timeElapsed)
	} else if timeElapsed > 60 {
		timeStr = color.YellowString("%f", timeElapsed)
	} else if timeElapsed > 40 {
		timeStr = color.BlueString("%f", timeElapsed)
	}

	log.Printf(boundedStrLayout, fmt.Sprintf("end of validation:\ntime is:  %s", timeStr))
	fileWithComments := ff.CellRegister.GetFileBytes()
	if fileWithComments != nil {
		destFile := fmt.Sprintf("%s_new_val_comm.xlsx", strings.TrimSuffix(filepath, ".xlsx"))
		//nolint:gosec
		if err = os.WriteFile(destFile, fileWithComments, 0666); err != nil {
			log.Fatalf("failed to save file with comments: %v", color.RedString(err.Error()))
		}
		log.Printf("file has been saved to %s ", color.BlackString(destFile))
	}

}
