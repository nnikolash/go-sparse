package sparse

import (
	"github.com/pkg/errors"
)

func NewSeriesSegment[Data any, Index any](
	storageFactory SeriesDataFactory[Data, Index],
	getIdx func(data *Data) Index,
	idxCmp func(idx1, idx2 Index) int,
	areContinuous func(smaller, bigger Index) bool,
) *SeriesSegment[Data, Index] {
	if areContinuous == nil {
		areContinuous = func(smaller, bigger Index) bool { return false }
	}

	return &SeriesSegment[Data, Index]{
		storageFactory: storageFactory,
		getIdx:         getIdx,
		idxCmp:         idxCmp,
		areContinuous:  areContinuous,
	}
}

type SeriesSegment[Data any, Index any] struct {
	storageFactory SeriesDataFactory[Data, Index]
	getIdx         func(data *Data) Index
	idxCmp         func(idx1, idx2 Index) int
	areContinuous  func(smaller, bigger Index) bool

	SeriesSegmentFields[Data, Index]
}

type PeriodBounds[Index any] struct {
	PeriodStart Index
	PeriodEnd   Index
}

type SeriesSegmentFields[Data any, Index any] struct {
	PeriodBounds[Index]
	Data  SeriesData[Data, Index]
	Empty bool
}

func (e *SeriesSegment[Data, Index]) GetAll() ([]Data, error) {
	if e.Data == nil {
		return nil, errors.New("data storage is not initialized")
	}

	return e.Data.Get(e.PeriodStart, e.PeriodEnd)
}

func (e *SeriesSegment[Data, Index]) GetAllInRange(minPeriodStart, maxPeriodEnd Index) (fetchedPeriodStart, fetchedPeriodEnd Index, data []Data, _ error) {
	if e.Data == nil {
		return fetchedPeriodStart, fetchedPeriodEnd, nil, errors.New("data storage is not initialized")
	}

	if e.idxCmp(e.PeriodEnd, minPeriodStart) < 0 || e.idxCmp(e.PeriodStart, maxPeriodEnd) > 0 {
		return fetchedPeriodStart, fetchedPeriodEnd, nil, errors.Errorf("period is out of range: segment = [ %v ; %v ], range = [ %v ; %v ]",
			e.PeriodStart, e.PeriodEnd, minPeriodStart, maxPeriodEnd)
	}

	periodStart := e.getBiggerIndex(e.PeriodStart, minPeriodStart)
	periodEnd := e.getSmallerIndex(e.PeriodEnd, maxPeriodEnd)

	data, err := e.Data.Get(periodStart, periodEnd)

	return periodStart, periodEnd, data, err
}

func (e *SeriesSegment[Data, Index]) First() (*Data, error) {
	if e.Data == nil {
		return nil, nil
	}

	return e.Data.First(e.PeriodStart)
}

func (e *SeriesSegment[Data, Index]) Last() (*Data, error) {
	if e.Data == nil {
		return nil, nil
	}

	return e.Data.Last(e.PeriodEnd)
}

func (e *SeriesSegment[Data, Index]) ContainsPoint(t Index) bool {
	if e.Data == nil {
		return false
	}

	return e.containsPoint(t)
}

func (e *SeriesSegment[Data, Index]) containsPoint(t Index) bool {
	return e.idxCmp(t, e.PeriodStart) >= 0 && e.idxCmp(t, e.PeriodEnd) <= 0
}

func (e *SeriesSegment[Data, Index]) CanBeMergedWith(t Index) bool {
	if e.Data == nil {
		return false
	}

	return e.containsPoint(t) || e.areContinuous(t, e.PeriodStart) || e.areContinuous(e.PeriodEnd, t)
}

func (e *SeriesSegment[Data, Index]) MergeData(data []Data) error {
	if len(data) == 0 {
		return nil
	}

	periodStart := e.getIdx(&data[0])
	periodEnd := e.getIdx(&data[len(data)-1])

	return e.MergePeriod(periodStart, periodEnd, data)
}

func (e *SeriesSegment[Data, Index]) MergePeriod(periodStart, periodEnd Index, data []Data) error {
	if err := e.validateDataBounds(periodStart, periodEnd, data); err != nil {
		return err
	}

	if e.Data == nil {
		storage, err := e.storageFactory(e.getIdx, e.idxCmp, periodStart, periodEnd, data)
		if err != nil {
			return err
		}

		e.Data = storage
		e.PeriodStart = periodStart
		e.PeriodEnd = periodEnd
		e.Empty = len(data) == 0

		return nil
	}

	if err := e.checkHaveIntersectionWithPeriod(periodStart, periodEnd); err != nil {
		return err
	}

	if err := e.Data.Merge(data); err != nil {
		return err
	}

	e.PeriodStart = e.getSmallerIndex(e.PeriodStart, periodStart)
	e.PeriodEnd = e.getBiggerIndex(e.PeriodEnd, periodEnd)
	e.Empty = e.Empty && len(data) == 0

	return nil
}

func (e *SeriesSegment[Data, Index]) validateDataBounds(periodStart, periodEnd Index, data []Data) error {
	if e.idxCmp(periodStart, periodEnd) > 0 {
		return errors.Errorf("incorrect period provided: %v > %v", periodStart, periodEnd)
	}

	if len(data) == 0 {
		return nil
	}

	dataPeriodStart := e.getIdx(&data[0])
	dataPeriodEnd := e.getIdx(&data[len(data)-1])

	if e.idxCmp(dataPeriodStart, dataPeriodEnd) > 0 {
		return errors.Errorf("data is not sorted: %v - %v", dataPeriodStart, dataPeriodEnd)
	}

	if e.idxCmp(periodStart, dataPeriodStart) > 0 {
		return errors.Errorf("incorrect period start: %v > %v", periodStart, dataPeriodStart)
	}
	if e.idxCmp(periodEnd, dataPeriodEnd) < 0 {
		return errors.Errorf("incorrect period end: %v < %v", periodEnd, dataPeriodEnd)
	}

	return nil
}

func (e *SeriesSegment[Data, Index]) checkHaveIntersectionWithPeriod(periodStart, periodEnd Index) error {
	if e.Data == nil {
		return nil
	}

	if e.idxCmp(periodStart, e.PeriodEnd) > 0 && !e.areContinuous(e.PeriodEnd, periodStart) {
		return errors.Errorf("merged data has no intersection with current data - merged period starts after current period end: merged = [ %v ; %v ], current = [ %v ; %v ]",
			periodStart, periodEnd, e.PeriodStart, e.PeriodEnd)
	}

	if e.idxCmp(periodEnd, e.PeriodStart) < 0 && !e.areContinuous(periodEnd, e.PeriodStart) {
		return errors.Errorf("merged data has no intersection with current data - merged period ends before current period end: merged = [ %v ; %v ], current = [ %v ; %v ]",
			periodStart, periodEnd, e.PeriodStart, e.PeriodEnd)
	}

	return nil
}

func (e *SeriesSegment[Data, Index]) getBiggerIndex(idx1, idx2 Index) Index {
	if e.idxCmp(idx1, idx2) > 0 {
		return idx1
	}

	return idx2
}

func (e *SeriesSegment[Data, Index]) getSmallerIndex(idx1, idx2 Index) Index {
	if e.idxCmp(idx1, idx2) < 0 {
		return idx1
	}

	return idx2
}

func (e *SeriesSegment[Data, Index]) Restore(f *SeriesSegmentFields[Data, Index]) {
	e.SeriesSegmentFields = *f
}

// func (e *SeriesEntry[Data, Index]) Validate() error {
// 	if e.Data == nil {
// 		return errors.New("data storage is not initialized")
// 	}

// 	if e.idxCmp(e.PeriodStart, e.PeriodEnd) > 0 {
// 		return errors.Errorf("incorrect period bounds: %v > %v", e.PeriodStart, e.PeriodEnd)
// 	}

// 	first, err := e.Data.First(e.PeriodStart)
// 	if err != nil {
// 		return errors.Wrap(err, "failed to get first data")
// 	}
// 	if first == nil {
// 		return nil
// 	}

//  QWE

// 	return nil
// }
