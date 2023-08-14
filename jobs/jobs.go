package jobs

import (
	"context"

	goxlsx "gitlab.ozon.ru/express/platform/lib/go-xlsx"
	"gitlab.ozon.ru/validator/goexel"
	"gitlab.ozon.ru/validator/platform"
)

type Entry struct {
	PromoName      goxlsx.String `xlsx:"Название промо"  xlsx-validation:"required"`
	PromoSubject   goxlsx.String `xlsx:"Тематика"`
	PromoDateFrom  goxlsx.Date   `xlsx:"Дата начала"     xlsx-validation:"required"`
	PromoDateTo    goxlsx.Date   `xlsx:"Дата окончания"  xlsx-validation:"required"`
	WhcClusterName goxlsx.String `xlsx:"География"       xlsx-validation:"required"`
	ItemID         goxlsx.Int64  `xlsx:"SKU"             xlsx-validation:"required"`
	ProviderID     goxlsx.Int64  `xlsx:"ID поставщика"   xlsx-validation:"required"`
	WarehouseID    goxlsx.Int64  `xlsx:"Склад"           xlsx-validation:"gtz"`
	Comment        goxlsx.String `xlsx:"Комментарий"`
	IsDelete       goxlsx.Bool
}

func init() {
	// add to the platform map
	// platform.JobMap[]
}

type SkuChecker struct {
	*platform.JobWrapper

	Exists map[int64]struct{}
}

func (j *SkuChecker) Run(ctx context.Context) (err error) {
	file := goexel.GetFileFromContext[Entry](ctx)

	checkerPlatformRes := j.Dependencies["СКУ В МАПЕ ЧЕКЕР"].Recv(ctx)
	if checkerPlatformRes.Err != nil {
		// можно и ошибку вернуть если что)
		return nil
	}

	checkerRes := checkerPlatformRes.Res.(IsSkuValidRes)

	for i, row := range file.Table {
		if checkerRes.Res[i] {
			if _, exists := j.Exists[row.ItemID.Value]; !exists {
				file.CellRegister.RegisterCellValueByString([]string{"СКУ НЕ В МАПЕ."}, row.Comment)
			}
		}
	}

	j.Done(ctx, platform.JobResult{})
	return nil
}

func (j *SkuChecker) GetDepIDs() []platform.JobID {
	return []platform.JobID{"Валидный ли Ску"}
}

func (j *SkuChecker) GetID() platform.JobID {
	return "СКУ В МАПЕ ЧЕКЕР"
}

func (j *SkuChecker) Copy() platform.Job {
	return &SkuChecker{
		JobWrapper: j.JobWrapper.Copy(),
		Exists:     j.Exists,
	}
}

// ---------------------------------------------------------------- IsSkuValid ----------------------------------------------------------------

type IsSkuValidRes struct {
	Res []bool
}

type IsSkuValid struct {
	*platform.JobWrapper
}

func (j *IsSkuValid) Run(ctx context.Context) (err error) {
	file := goexel.GetFileFromContext[Entry](ctx)

	res := IsSkuValidRes{Res: make([]bool, len(file.Table))}

	for i, row := range file.Table {
		if row.ItemID.IsEmpty() {
			file.CellRegister.RegisterCellValueByString([]string{"Пустой Ску."}, row.Comment)
			continue
		}
		res.Res[i] = true
	}

	j.Done(ctx, platform.JobResult{Res: res})
	return nil
}

func (j *IsSkuValid) GetDepIDs() []platform.JobID {
	return nil
}

func (j *IsSkuValid) GetID() platform.JobID {
	return "Валидный ли Ску"
}

func (j *IsSkuValid) Copy() platform.Job {
	return &IsSkuValid{
		JobWrapper: j.JobWrapper.Copy(),
	}
}
