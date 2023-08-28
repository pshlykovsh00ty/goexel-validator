package jobs

import (
	"context"
	"fmt"
	"time"

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

		time.Sleep(20 * time.Millisecond)

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

		time.Sleep(20 * time.Millisecond)
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

func (j *FunValidation) Create() platform.Job {
	return &FunValidation{
		JobWrapper: j.JobWrapper.Create(),
	}
}
