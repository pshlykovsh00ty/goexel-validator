package jobs

import (
	"context"
	"fmt"
	"sort"
	"strings"

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

	PromoMechanics          goxlsx.String  `xlsx:"Промо механика"`
	Coefficient             goxlsx.Float64 `xlsx:"Коэффициент"`
	UseInPromo              goxlsx.Bool    `xlsx:"Дублирование из тематики в промо" xlsx-format:"Да"`
	Price                   goxlsx.Float64 `xlsx:"Закупочная регулярная цена без НДС, руб" xlsx-validation:"gtez"`
	PromoPrice              goxlsx.Float64 `xlsx:"Закупка в промо без НДС, руб"            xlsx-validation:"gtez"`
	NDSRate                 goxlsx.Int32   `xlsx:"Ставка НДС, %"`
	PromoPriceListDateFrom  goxlsx.Date    `xlsx:"Начало действия закупочной цены в промо"`
	PromoPriceListDateTo    goxlsx.Date    `xlsx:"Окончание действия закупочной цены в промо"`
	DiscountOffPercent      goxlsx.Float64 `xlsx:"Скидка off, %"`
	DiscountOffRUR          goxlsx.Float64 `xlsx:"Скидка off, руб"                         xlsx-validation:"gtz"`
	Volume                  goxlsx.Int32   `xlsx:"Объем"                          xlsx-validation:"gtz"`
	PromoType               goxlsx.String  `xlsx:"Тип промо"   xlsx-validation:"required"`
	PurchaseType            goxlsx.String  `xlsx:"Тип Закупки"   xlsx-validation:"required"`
	SupplierCompensation    goxlsx.String  `xlsx:"Компенсация поставщика"`
	SupplierCompensationSum goxlsx.Float64 `xlsx:"Сумма компенсации от поставщика в рублях"   xlsx-validation:"gtez"`
	RecommendedPrice        goxlsx.Float64 `xlsx:"Рекомендованные цены от КМ"   xlsx-validation:"gtez"`
	SamplingObligation      goxlsx.Bool    `xlsx:"Обязательство выборки" xlsx-format:"Да"`
	PositionAttribute       goxlsx.String  `xlsx:"Признак позиции"   xlsx-validation:"required"`
	PriceListID             goxlsx.String  `xlsx:"ID прайс-листа"`
	JiraID                  goxlsx.String  `xlsx:"Заявка в JIRA"`
	SoftErrors              goxlsx.String  `xlsx:"Ошибка"` //TODO нейминг можно поправить уже после рефакторинга, сейчас тут попадаются не только "не строгие" ошибки
}

func (e Entry) GetItemID() int64 {
	return e.ItemID.Value
}

type SkuChecker struct {
	*platform.JobWrapper

	Exists map[int64]struct{}
}

func (j *SkuChecker) Run(ctx context.Context) (err error) {

	checkerResChan, ok := j.Dependencies["Валидный ли Ску"]
	if !ok {
		return fmt.Errorf("%w: no checker chanel", platform.ErrFatal)
	}

	return platform.RunByLine[Entry](ctx, j.JobWrapper, func(c context.Context, register *goexel.FileCellRegisterer, row *Entry) platform.JobResult {
		checkerRes := <-checkerResChan

		if checkerRes.Err != nil {
			return platform.JobResult{Err: platform.ErrSkipped}
		}
		if isValidSKU := checkerRes.Res.(bool); !isValidSKU {
			register.RegisterCellValueByString([]string{"Знаю что 1."}, row.Comment)
			return platform.JobResult{Err: platform.ErrSkipped}
		}

		_, exists := j.Exists[row.ItemID.Value]
		if !exists {
			register.RegisterCellValueByString([]string{"СКУ НЕ В МАПЕ."}, row.Comment)
		}
		return platform.JobResult{Res: exists}
	})
}

func (j *SkuChecker) GetDepIDs() []platform.JobID {
	return []platform.JobID{"Валидный ли Ску"}
}

func (j *SkuChecker) GetID() platform.JobID {
	return "СКУ В МАПЕ ЧЕКЕР"
}

func (j *SkuChecker) GetType() platform.JobType {
	return platform.Common
}

func (j *SkuChecker) Create() platform.Job {
	return &SkuChecker{
		JobWrapper: j.JobWrapper.Create(),
		Exists:     j.Exists,
	}
}

// ---------------------------------------------------------------- IsSkuValid ----------------------------------------------------------------

type IsSkuValid struct {
	*platform.JobWrapper
}

func (j *IsSkuValid) Run(ctx context.Context) (err error) {

	return platform.RunByLine[Entry](ctx, j.JobWrapper, func(c context.Context, register *goexel.FileCellRegisterer, row *Entry) platform.JobResult {
		isEmpty := row.ItemID.IsEmpty() || row.ItemID.Value == 1
		if isEmpty {
			register.RegisterCellValueByString([]string{"Пустой Ску."}, row.Comment)
		}
		return platform.JobResult{
			Res: !isEmpty,
		}
	})
}

func (j *IsSkuValid) GetDepIDs() []platform.JobID {
	return nil
}

func (j *IsSkuValid) GetID() platform.JobID {
	return "Валидный ли Ску"
}

func (j *IsSkuValid) GetType() platform.JobType {
	return platform.Common
}

func (j *IsSkuValid) Create() platform.Job {
	return &IsSkuValid{
		JobWrapper: j.JobWrapper.Create(),
	}
}

// ---------------------------------------------------------------- Data validation ----------------------------------------------------------------

type DataValidation struct {
	*platform.JobWrapper
}

func (j *DataValidation) Run(ctx context.Context) (err error) {

	return platform.RunByLine[Entry](ctx, j.JobWrapper, func(c context.Context, register *goexel.FileCellRegisterer, row *Entry) platform.JobResult {
		rowNumber := row.PromoName.GetRowNumber()

		if !row.PromoDateFrom.IsValid() {
			register.RegisterCommentByRow(fmt.Sprintf("Невалидное поле Дата начала %s", row.PromoDateFrom.Value), rowNumber)
			return platform.JobResult{Res: false}
		}
		if !row.PromoDateTo.IsValid() {
			register.RegisterCommentByRow(fmt.Sprintf("Невалидное поле Дата окончания %s", row.PromoDateTo.Value), rowNumber)
			return platform.JobResult{Res: false}
		}

		if row.PromoDateTo.Value.Before(row.PromoDateFrom.Value) {
			register.RegisterCommentByRow("Дата начала промо не может быть после даты окончания", rowNumber)
			return platform.JobResult{Res: false}
		}
		return platform.JobResult{
			Res: true,
		}
	})
}

func (j *DataValidation) GetDepIDs() []platform.JobID {
	return nil
}

func (j *DataValidation) GetID() platform.JobID {
	return "Влидация дат начала и конца промо акции"
}

func (j *DataValidation) GetType() platform.JobType {
	return platform.Common
}

func (j *DataValidation) Create() platform.Job {
	return &DataValidation{
		JobWrapper: j.JobWrapper.Create(),
	}
}

// ---------------------------------------------------------------- Just for fun validation ----------------------------------------------------------------

type FunValidation struct {
	*platform.JobWrapper
}

func (j *FunValidation) Run(ctx context.Context) (err error) {

	isValidSkuChan, ok := j.Dependencies["Валидный ли Ску"]
	if !ok {
		return fmt.Errorf("%w: no checker chanel", platform.ErrFatal)
	}

	dataChekerChan, ok := j.Dependencies["Влидация дат начала и конца промо акции"]
	if !ok {
		return fmt.Errorf("%w: no date validation chanel", platform.ErrFatal)
	}

	return platform.RunByLine[Entry](ctx, j.JobWrapper, func(c context.Context, register *goexel.FileCellRegisterer, row *Entry) platform.JobResult {

		isValidSkuRes := <-isValidSkuChan
		if isValidSkuRes.Err != nil {
			return isValidSkuRes
		}
		isValidSku := isValidSkuRes.Res.(bool)

		if isValidSku {
			isVaidDataRes := <-dataChekerChan
			if isVaidDataRes.Err != nil {
				return isVaidDataRes
			}
			if isValidData := isVaidDataRes.Res.(bool); isValidData {
				register.RegisterCellValueByString([]string{"Фановая валидация нашла валидную дату)."}, row.SoftErrors)
				return platform.JobResult{
					Res: true,
				}
			}
		}

		return platform.JobResult{
			Res: false,
		}
	})
}

func (j *FunValidation) GetDepIDs() []platform.JobID {
	return []platform.JobID{"Влидация дат начала и конца промо акции", "Валидный ли Ску"}
}

func (j *FunValidation) GetID() platform.JobID {
	return "Проверяем двойные зависимости"
}

func (j *FunValidation) GetType() platform.JobType {
	return platform.Common
}

func (j *FunValidation) Create() platform.Job {
	return &FunValidation{
		JobWrapper: j.JobWrapper.Create(),
	}
}

// ---------------------------------------------------------------- sorting  ----------------------------------------------------------------

type Sorting struct {
	*platform.JobWrapper
}

func (j *Sorting) Run(ctx context.Context) (err error) {
	file := goexel.GetFileFromContext[Entry](ctx)
	sort.Slice(file.Table, func(i, j int) bool {
		return file.Table[i].ItemID.Value < file.Table[j].ItemID.Value
	})
	// пишущая джоба не отправляет никому ничего, но можно сделать для профилактики отправкуы
	j.Send(ctx, platform.JobResult{})
	return nil
}

func (j *Sorting) GetDepIDs() []platform.JobID {
	return nil
}

func (j *Sorting) GetID() platform.JobID {
	return "Сортируем по скухам"
}

func (j *Sorting) GetType() platform.JobType {
	return platform.Writer
}

func (j *Sorting) Create() platform.Job {
	return &Sorting{
		JobWrapper: j.JobWrapper.Create(),
	}
}

// ---------------------------------------------------------------- batch volume validation  ----------------------------------------------------------------

type BatchVolumeValidation struct {
	*platform.JobWrapper
	PrivilegedClusters map[string]struct{}
}

func (j *BatchVolumeValidation) Run(ctx context.Context) (err error) {

	clusterChan, exists := j.Dependencies["Валидация кластеров"]
	if !exists {
		return fmt.Errorf("%w: no cluster channel", platform.ErrFatal)
	}
	var (
		clusterVolumes  = make(map[string]int32, 10)
		wrongPrivileged = make([]string, 0, 2)
	)
	return platform.RunByItemBatch(ctx, j.JobWrapper, func(c context.Context, register *goexel.FileCellRegisterer, rows []*Entry) platform.JobResult {
		for _, row := range rows {
			jobResult := <-clusterChan
			if jobResult.Err != nil {
				continue
			}

			cluster := jobResult.Res.(string)
			clusterVolumes[cluster] += row.Volume.Value
		}

		for cluster, vol := range clusterVolumes {

			if _, exists := j.PrivilegedClusters[cluster]; exists {
				continue
			}
			for pc := range j.PrivilegedClusters {
				pvol, exists := clusterVolumes[pc]
				if !exists {
					continue
				}
				if vol > pvol {
					wrongPrivileged = append(wrongPrivileged, pc)
				}
			}
			if len(wrongPrivileged) != 0 {
				for _, row := range rows {
					if row.WhcClusterName.Value == cluster {
						register.RegisterCellValueByString([]string{
							fmt.Sprintf(
								"%s имеет объем больше чем в %s",
								cluster, strings.Join(wrongPrivileged, ", "),
							),
						}, row.SoftErrors)
					}
				}
			}
			wrongPrivileged = wrongPrivileged[:0]
		}
		for k := range clusterVolumes {
			delete(clusterVolumes, k)
		}
		return platform.JobResult{}
	})
}

func (j *BatchVolumeValidation) GetDepIDs() []platform.JobID {
	return []platform.JobID{"Сортируем по скухам", "Валидация кластеров"}
}

func (j *BatchVolumeValidation) GetID() platform.JobID {
	return "Относительный объем"
}

func (j *BatchVolumeValidation) GetType() platform.JobType {
	return platform.Common
}

func (j *BatchVolumeValidation) Create() platform.Job {
	return &BatchVolumeValidation{
		PrivilegedClusters: j.PrivilegedClusters,
		JobWrapper:         j.JobWrapper.Create(),
	}
}

// ---------------------------------------------------------------- cluster validation  ----------------------------------------------------------------

type IsClusterValid struct {
	*platform.JobWrapper

	ValidClusters map[string]struct{}
}

func (j *IsClusterValid) Run(ctx context.Context) (err error) {

	return platform.RunByLine[Entry](ctx, j.JobWrapper, func(c context.Context, register *goexel.FileCellRegisterer, row *Entry) platform.JobResult {

		if !row.WhcClusterName.IsEmpty() && row.WhcClusterName.IsValid() {
			if _, exists := j.ValidClusters[row.WhcClusterName.Value]; exists {
				return platform.JobResult{
					Res: row.WhcClusterName.Value,
				}
			}
		}
		register.RegisterCommentByValue(&row.WhcClusterName, "Невалидный кластер")
		return platform.JobResult{
			Err: platform.ErrSkipped,
		}
	})
}

func (j *IsClusterValid) GetDepIDs() []platform.JobID {
	return nil
}

func (j *IsClusterValid) GetID() platform.JobID {
	return "Валидация кластеров"
}

func (j *IsClusterValid) GetType() platform.JobType {
	return platform.Common
}

func (j *IsClusterValid) Create() platform.Job {
	return &IsClusterValid{
		JobWrapper:    j.JobWrapper.Create(),
		ValidClusters: j.ValidClusters,
	}
}
