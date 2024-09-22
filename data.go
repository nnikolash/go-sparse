package sparse

import (
	"fmt"
	"slices"
	"sort"
)

type SeriesData[Data any, Index any] interface {
	Get(periodStart, periodEnd Index) ([]Data, error)
	GetEndOpen(periodStart, periodEnd Index) ([]Data, error)
	Merge(data []Data) error
	First(idx Index) (*Data, error)
	Last(idx Index) (*Data, error)
	//String() string

	// TODO: Delete method (e.g. to use for cleanup of entries in db)
}

type SeriesDataFactory[Data any, Index any] func(
	getIdx func(data *Data) Index,
	idxCmp func(idx1, idx2 Index) int,
	periodStart, periodEnd Index, data []Data,
) (SeriesData[Data, Index], error)

func NewArrayData[Data any, Index any](
	getIdx func(data *Data) Index,
	idxCmp func(idx1, idx2 Index) int,
	periodStart, periodEnd Index, data []Data,
) (SeriesData[Data, Index], error) {
	return &ArrayData[Data, Index]{
		getIdx: getIdx,
		idxCmp: idxCmp,
		data:   data,
	}, nil
}

var _ SeriesDataFactory[int, int] = NewArrayData

type ArrayData[Data any, Index any] struct {
	getIdx func(data *Data) Index
	idxCmp func(idx1, idx2 Index) int
	data   []Data
}

func (s *ArrayData[Data, Index]) First(idx Index) (*Data, error) {
	if len(s.data) == 0 {
		return nil, nil
	}

	v := s.data[0]

	return &v, nil
}

func (s *ArrayData[Data, Index]) Last(idx Index) (*Data, error) {
	if len(s.data) == 0 {
		return nil, nil
	}

	v := s.data[len(s.data)-1]

	return &v, nil
}

func (s *ArrayData[Data, Index]) Get(periodStart, periodEnd Index) ([]Data, error) {
	return s.get(periodStart, periodEnd, false)
}

func (s *ArrayData[Data, Index]) GetEndOpen(periodStart, periodEnd Index) ([]Data, error) {
	res, err := s.get(periodStart, periodEnd, true)
	return res, err
}

func (s *ArrayData[Data, Index]) get(periodStart, periodEnd Index, endOpen bool) ([]Data, error) {
	dataStartIdx := s.getStartIdx(periodStart)

	var dataEndIdx int
	if endOpen {
		dataEndIdx = s.getEndIdxOpen(periodEnd)
	} else {
		dataEndIdx = s.getEndIdx(periodEnd)
	}

	if dataStartIdx > dataEndIdx {
		return []Data{}, nil
	}

	if dataStartIdx == -1 || dataEndIdx == -1 {
		panic(fmt.Errorf("data is not sorted - could find range [%v; %v] inside data %v: dataStartIdx = %v, dataEndIdx = %v",
			periodStart, periodEnd, s.data, dataStartIdx, dataEndIdx))
	}

	if dataEndIdx == len(s.data) {
		panic(fmt.Errorf("data is not sorted - could not find end index: periodEnd = %v, data = %v", periodEnd, s.data))
	}

	return s.data[dataStartIdx : dataEndIdx+1 : dataEndIdx+1], nil
}

func (s *ArrayData[Data, Index]) getStartIdx(periodStart Index) int {
	startIdx := sort.Search(len(s.data), func(i int) bool {
		idx := s.getIdx(&s.data[i])
		return s.idxCmp(idx, periodStart) >= 0
	})

	return startIdx
}

func (s *ArrayData[Data, Index]) getEndIdx(periodEnd Index) int {
	endIdx := sort.Search(len(s.data), func(i int) bool {
		idx := s.getIdx(&s.data[i])
		return s.idxCmp(idx, periodEnd) > 0
	})

	return endIdx - 1
}

func (s *ArrayData[Data, Index]) getEndIdxOpen(periodEnd Index) int {
	endIdx := sort.Search(len(s.data), func(i int) bool {
		idx := s.getIdx(&s.data[i])
		return s.idxCmp(idx, periodEnd) >= 0
	})

	return endIdx - 1
}

func (s *ArrayData[Data, Index]) Merge(data []Data) error {
	if len(data) == 0 {
		return nil
	}

	if len(s.data) == 0 {
		s.data = slices.Clone(data)
		return nil
	}

	var oldDataBeforeNewData []Data
	var oldDataAfterNewData []Data

	oldDataStart := s.getIdx(&s.data[0])
	newDataStart := s.getIdx(&data[0])

	if s.idxCmp(newDataStart, oldDataStart) > 0 {
		lastOldBeforeNew := s.getStartIdx(newDataStart)
		oldDataBeforeNewData = s.data[:lastOldBeforeNew]
	}

	oldDataEnd := s.getIdx(&s.data[len(s.data)-1])
	newDataEnd := s.getIdx(&data[len(data)-1])

	if s.idxCmp(newDataEnd, oldDataEnd) < 0 {
		firstOldAfterNew := s.getEndIdx(newDataEnd) + 1
		oldDataAfterNewData = s.data[firstOldAfterNew:]
	}

	if len(oldDataBeforeNewData) == 0 && len(oldDataAfterNewData) == 0 {
		s.data = slices.Clone(data)
		return nil
	}

	// TODO: how to do it more effective? how to use existing capacity?
	merged := make([]Data, 0, len(oldDataBeforeNewData)+len(data)+len(oldDataAfterNewData))
	merged = append(merged, oldDataBeforeNewData...)
	merged = append(merged, data...)
	merged = append(merged, oldDataAfterNewData...)
	s.data = merged

	return nil
}

func (s *ArrayData[Data, Index]) String() string {
	return fmt.Sprintf("%v", s.data)
}
