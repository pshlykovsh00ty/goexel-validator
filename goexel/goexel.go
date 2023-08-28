package goexel

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/xuri/excelize/v2"
	goxlsx "gitlab.ozon.ru/express/platform/lib/go-xlsx"
	"gitlab.ozon.ru/platform/tracer-go/logger"
)

func SetFileContext[T any](ctx context.Context, f *File[T]) context.Context {
	return context.WithValue(ctx, ctxkey, f)
}

func GetFileFromContext[T any](ctx context.Context) *File[T] {
	fileIntf := ctx.Value(ctxkey)
	file := fileIntf.(*File[T])
	return file
}

type ctxFileKey int8

const (
	ctxkey ctxFileKey = 1
)

type File[T any] struct {
	Table        []*T
	CellRegister *FileCellRegisterer
}

func NewFile[T any](file []byte) (*File[T], error) {
	reader := bytes.NewReader(file)
	f, err := excelize.OpenReader(reader)
	if err != nil {
		return nil, errors.Wrap(err, "Ошибка чтения файла")
	}

	decoder := goxlsx.NewDecoder(f)
	commentRegister := goxlsx.NewValidationRegister(f)
	register, err := NewFileRegisterer(f, commentRegister)
	if err != nil {
		return nil, err
	}

	decoder.AddRegister(commentRegister)
	resArr := make([]*T, 0)
	decoder.Decode(&resArr)

	return &File[T]{
		Table:        resArr,
		CellRegister: register,
	}, nil
}

// FileCellRegisterer - сущность которая добавляет строковые значения прямиком в ячейки таблицы.
// Если не использовать в конце SaveValuesToFile
type FileCellRegisterer struct {
	sheet             string
	cellValues        map[string][]string
	cellValMu         *sync.Mutex
	file              *excelize.File
	style             int
	commentRegisterer *goxlsx.ValidationRegister
	commMu            *sync.Mutex
}

// GetFileBytes - записывает все комментарии и значения ячеек в файл, а затем отдает его байты
func (f FileCellRegisterer) GetFileBytes() []byte {
	f.commMu.Lock()
	defer f.commMu.Unlock()
	f.saveValuesToFile(context.Background())
	return f.commentRegisterer.GetFileBytesWithComments()
}

// AddCommentsAuthor - добавляет автора к комментариям по валидациям
func (f FileCellRegisterer) AddCommentsAuthor(author string) *goxlsx.ValidationRegister {
	f.commMu.Lock()
	defer f.commMu.Unlock()
	return f.commentRegisterer.AddAuthor(author)
}

// HasComments - показывает были ли добавлены комментарии с замечаниями
func (f FileCellRegisterer) HasComments() bool {
	f.commMu.Lock()
	defer f.commMu.Unlock()
	return f.commentRegisterer.HasComments()
}

// RegisterComment -  добавляет комментарий к первой ячейке шаблона по дефолтной странице
func (f FileCellRegisterer) RegisterComment(message string) {
	f.commMu.Lock()
	f.commentRegisterer.Register(message)
	f.commMu.Unlock()
}

// RegisterCommentByCol добавляет комментарий к колонке первой записи листа
func (f FileCellRegisterer) RegisterCommentByCol(message string, col int) {
	f.commMu.Lock()
	f.commentRegisterer.RegisterByCol(f.sheet, message, col)
	f.commMu.Unlock()
}

// RegisterCommentByPosition добавляет комментарий к ячейке шаблона
func (f FileCellRegisterer) RegisterCommentByPosition(message string, col int, row int) {
	f.commMu.Lock()
	f.commentRegisterer.RegisterByPosition(f.sheet, message, col, row)
	f.commMu.Unlock()
}

// RegisterCommentByRow добавляет комментарий к строке первой колонки листа
func (f FileCellRegisterer) RegisterCommentByRow(message string, row int) {
	f.commMu.Lock()
	f.commentRegisterer.RegisterByRow(f.sheet, message, row)
	f.commMu.Unlock()
}

// RegisterCommentBySheet - добавляет комментарий к первой ячейке шаблона
func (f FileCellRegisterer) RegisterCommentBySheet(message string) {
	f.commMu.Lock()
	f.commentRegisterer.RegisterBySheet(f.sheet, message)
	f.commMu.Unlock()
}

// RegisterCommentByValue - добавляет комментарий к ячейке шаблона
func (f FileCellRegisterer) RegisterCommentByValue(value goxlsx.Type, message string) {
	f.commMu.Lock()
	f.commentRegisterer.RegisterByValue(value, message)
	f.commMu.Unlock()
}

// RegisterCommentNotExist - добавляет комментарий, что данные о записи не найдены
func (f FileCellRegisterer) RegisterCommentNotExist(value goxlsx.Type) {
	f.commMu.Lock()
	f.commentRegisterer.RegisterNotExist(value)
	f.commMu.Unlock()
}

// SetSheet -  необходимо перед всем командами register (кроме импользующих goxlsx.Type) и просто RegisterComment
func (f *FileCellRegisterer) SetSheet(sheet string) {
	f.cellValMu.Lock()
	defer f.cellValMu.Unlock()
	f.sheet = sheet
}

// RegisterCellValueByString - добавляет строки в ячейку шаблона
func (f *FileCellRegisterer) RegisterCellValueByString(messages []string, value goxlsx.String) {
	f.RegisterCellValueByPosition(messages, value.GetColumnNumber(), value.GetRowNumber())
}

// RegisterCellValueByPosition добавляет строки прямиком к ячейке с колонкой и строкой
func (f *FileCellRegisterer) RegisterCellValueByPosition(messages []string, col, row int) {
	f.cellValMu.Lock()
	if col == 0 {
		col = 1
	}
	if row == 0 {
		row = 1
	}

	column, _ := excelize.ColumnNumberToName(col)
	cell := column + strconv.Itoa(row)
	f.cellValues[cell] = append(f.cellValues[cell], messages...)
	f.cellValMu.Unlock()
}

func (f *FileCellRegisterer) saveValuesToFile(ctx context.Context) {
	for cell, value := range f.cellValues {
		if err := f.file.SetCellStyle(f.sheet, cell, cell, f.style); err != nil {
			logger.Errorf(ctx, "failed to set cell style: %v", err)
		}
		if err := f.file.SetCellStr(f.sheet, cell, strings.Join(value, "\n ")); err != nil {
			logger.Errorf(ctx, "failed to set cell value: %v", err)
		}
	}
}

// HasCellEntries - показывает были ли сделаны записи прямиком в ячейки
func (f *FileCellRegisterer) HasCellEntries() bool {
	f.cellValMu.Lock()
	defer f.cellValMu.Unlock()
	return len(f.cellValues) > 0
}

var defaultCellStyle = &excelize.Style{
	Font: &excelize.Font{
		Family: "Calibri",
		Size:   11,
		Color:  "#000000",
	},
}

type opt func(f *FileCellRegisterer)

// WithCellStyle - опция устанавливающая шрифт для текста в ячейчах
func WithCellStyle(cellStyle *excelize.Style) opt {
	return func(f *FileCellRegisterer) {
		if cellStyle == nil {
			return
		}
		style, err := f.file.NewStyle(cellStyle)
		if err != nil {
			logger.Errorf(context.Background(), "failed to set cell style: %v")
		}
		f.style = style
	}
}

// NewFileRegisterer - создает сущность, которая в file записывает строки в ячейки или в комментарии к ним
func NewFileRegisterer(file *excelize.File, commentRegisterer *goxlsx.ValidationRegister, opts ...opt) (*FileCellRegisterer, error) {
	f := &FileCellRegisterer{
		cellValMu:         &sync.Mutex{},
		cellValues:        make(map[string][]string),
		file:              file,
		commentRegisterer: commentRegisterer,
		commMu:            &sync.Mutex{},
	}
	for _, opt := range opts {
		opt(f)
	}
	if f.style == 0 {
		style, err := f.file.NewStyle(defaultCellStyle)
		if err != nil {
			return nil, err
		}
		f.style = style
	}
	return f, nil
}
