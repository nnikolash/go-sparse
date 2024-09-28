package sparse

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

func NewSeries[Data any, Index any](
	storageFactory SeriesDataFactory[Data, Index],
	getIdx func(data *Data) Index,
	cmp func(idx1, idx2 Index) int,
	areContinuous func(smaller, bigger Index) bool,
) *Series[Data, Index] {
	if cmp == nil {
		cmp = CreateComparatorAny[Index]()
	}
	if areContinuous == nil {
		areContinuous = func(smaller, bigger Index) bool { return false }
	}

	return &Series[Data, Index]{
		dataFactory:   storageFactory,
		getIdx:        getIdx,
		idxCmp:        cmp,
		areContinuous: areContinuous,
	}
}

type Series[Data any, Index any] struct {
	dataFactory   SeriesDataFactory[Data, Index]
	getIdx        func(data *Data) Index
	idxCmp        func(idx1, idx2 Index) int
	areContinuous func(smaller, bigger Index) bool
	segments      []*SeriesSegment[Data, Index]
}

func (s *Series[Data, Index]) Segments() []*SeriesSegment[Data, Index] {
	return s.segments
}

// For debugging purposes
func (s *Series[Data, Index]) SegmentsString() string {
	var res strings.Builder
	for _, segment := range s.segments {
		if res.Len() > 0 {
			res.WriteString("| ")
		}

		data, err := segment.Data.Get(segment.PeriodStart, segment.PeriodEnd)
		if err != nil {
			panic(fmt.Errorf("could not get data: %v", err))
		}

		for _, elem := range data {
			res.WriteString(fmt.Sprintf("%v ", s.getIdx(&elem)))
		}
	}

	return res.String()
}

func (s *Series[Data, Index]) GetAllSegments() []*SeriesSegment[Data, Index] {
	if len(s.segments) == 0 {
		return nil
	}

	return s.segments
}

func (s *Series[Data, Index]) GetSegment(t Index) *SeriesSegment[Data, Index] {
	segmentIdx, contains := s.findSegmentWhichStartsBeforeOrAt(t, false)
	if segmentIdx == -1 && !contains {
		return nil
	}

	return s.segments[segmentIdx]
}

func (s *Series[Data, Index]) Get(periodStart, periodEnd Index) ([]Data, error) {
	if len(s.segments) == 0 {
		return nil, errors.WithStack(&MissingPeriodError[Index]{PeriodStart: periodStart, PeriodEnd: periodEnd})
	}
	if s.idxCmp(periodStart, periodEnd) > 0 {
		return nil, errors.Errorf("requested period start is greater than period end: %v > %v", periodStart, periodEnd)
	}

	intersectLastSegmentIdx, lastContains := s.findSegmentWhichStartsBeforeOrAt(periodEnd, false)
	if intersectLastSegmentIdx == -1 {
		return nil, errors.WithStack(&MissingPeriodError[Index]{PeriodStart: periodStart, PeriodEnd: periodEnd})
	}

	intersectFirstSegmentIdx, firstContains := s.findSegmentWhichStartsBeforeOrAt(periodStart, false)
	if intersectFirstSegmentIdx == -1 {
		currentBeginning := s.segments[0].PeriodStart
		return nil, errors.WithStack(&MissingPeriodError[Index]{PeriodStart: periodStart, PeriodEnd: currentBeginning})
	}

	intersectFirstSegment := s.segments[intersectFirstSegmentIdx]
	intersectLastSegment := s.segments[intersectLastSegmentIdx]

	if intersectFirstSegmentIdx != intersectLastSegmentIdx || !firstContains || !lastContains {
		if firstContains {
			periodStart = intersectFirstSegment.PeriodEnd
		}
		if lastContains {
			periodEnd = intersectLastSegment.PeriodStart
		}
		return nil, errors.WithStack(&MissingPeriodError[Index]{PeriodStart: periodStart, PeriodEnd: periodEnd})
	}

	segment := intersectFirstSegment

	data, err := segment.Data.Get(periodStart, periodEnd)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	firstIdx := s.getIdx(&data[0])
	if s.idxCmp(firstIdx, periodStart) < 0 {
		return nil, errors.Errorf("storage error: data is not sorted: firstIdx > periodStart: %v > %v", firstIdx, periodStart)
	}

	lastIdx := s.getIdx(&data[len(data)-1])
	if s.idxCmp(lastIdx, periodEnd) > 0 {
		return nil, errors.Errorf("storage error: data is not sorted: lastIdx < periodEnd: %v < %v", lastIdx, periodEnd)
	}

	return data, nil
}

func (s *Series[Data, Index]) GetPeriod(periodStart, periodEnd Index) *SeriesSegment[Data, Index] {
	if len(s.segments) == 0 {
		return nil
	}

	firstSegmentIdx, contains := s.findSegmentWhichStartsBeforeOrAt(periodStart, false)
	if firstSegmentIdx == -1 || !contains {
		return nil
	}

	if s.idxCmp(periodStart, periodEnd) == 0 {
		return s.segments[firstSegmentIdx]
	}

	lastSegmentIdx, contains := s.findSegmentWhichStartsBeforeOrAt(periodEnd, false)
	if lastSegmentIdx == -1 || !contains {
		return nil
	}

	if firstSegmentIdx != lastSegmentIdx {
		return nil
	}

	return s.segments[firstSegmentIdx]
}

func (s *Series[Data, Index]) GetPeriodClosestFromStart(t Index, nonEmpty bool) *SeriesSegment[Data, Index] {
	if len(s.segments) == 0 {
		return nil
	}

	segmentIdx, _ := s.findSegmentWhichStartsBeforeOrAt(t, false)
	if segmentIdx == -1 {
		return nil
	}

	if nonEmpty {
		// TODO: this is ineffective... but I just hope there will be not much of empty segments
		for segmentIdx >= 0 && s.segments[segmentIdx].Empty {
			segmentIdx--
		}
		if segmentIdx == -1 {
			return nil
		}
	}

	return s.segments[segmentIdx]
}

func (s *Series[Data, Index]) GetPeriodClosestFromEnd(t Index, nonEmpty bool) *SeriesSegment[Data, Index] {
	if len(s.segments) == 0 {
		return nil
	}

	segmentIdx, contains := s.findSegmentWhichStartsBeforeOrAt(t, false)
	if segmentIdx == -1 {
		segmentIdx = 0
	} else if !contains {
		if segmentIdx == len(s.segments)-1 {
			return nil
		}
		segmentIdx++
	}

	if nonEmpty {
		// TODO: this is ineffective... but I just hope there will be not much of empty segments
		for segmentIdx < len(s.segments) && s.segments[segmentIdx].Empty {
			segmentIdx++
		}
		if segmentIdx == len(s.segments) {
			return nil
		}
	}

	return s.segments[segmentIdx]
}

func (s *Series[Data, Index]) AddData(data []Data) error {
	if len(data) == 0 {
		return nil
	}

	periodStart := s.getIdx(&data[0])
	periodEnd := s.getIdx(&data[len(data)-1])

	return s.AddPeriod(periodStart, periodEnd, data)
}

func (s *Series[Data, Index]) AddPeriod(periodStart, periodEnd Index, data []Data) error {
	if len(s.segments) == 0 {
		newSegment := NewSeriesSegment[Data, Index](s.dataFactory, s.getIdx, s.idxCmp, s.areContinuous)

		if err := newSegment.MergePeriod(periodStart, periodEnd, data); err != nil {
			return err
		}

		s.segments = append(s.segments, newSegment)

		return nil
	}

	intersectFirstSegmentIdx, firstContains := s.findSegmentWhichStartsBeforeOrAt(periodStart, true)
	intersectLastSegmentIdx, lastContains := s.findSegmentWhichStartsBeforeOrAt(periodEnd, true)

	endsBeforeStart := intersectLastSegmentIdx == -1
	if endsBeforeStart {
		return s.insertBeforeStart(periodStart, periodEnd, data)
	}

	startsBeforeStart := intersectFirstSegmentIdx == -1
	if startsBeforeStart {
		return s.mergeWithStart(periodStart, periodEnd, data, intersectLastSegmentIdx)
	}

	lastSegmentIdx := len(s.segments) - 1

	if intersectFirstSegmentIdx == lastSegmentIdx && !firstContains {
		return s.insertAfterEnd(periodStart, periodEnd, data)
	}

	if intersectLastSegmentIdx == lastSegmentIdx && !lastContains {
		return s.mergeWithEnd(periodStart, periodEnd, data, intersectFirstSegmentIdx)
	}

	return s.mergeWithinRange(periodStart, periodEnd, data, intersectFirstSegmentIdx, intersectLastSegmentIdx)
}

func (s *Series[Data, Index]) Restore(state *SeriesState[Data, Index]) error {
	for _, segment := range state.Segments {
		if s.idxCmp(segment.PeriodStart, segment.PeriodEnd) > 0 {
			return errors.Errorf("storage error: segment period start is greater than period end: %v > %v", segment.PeriodStart, segment.PeriodEnd)
		}
	}

	segments := make([]*SeriesSegment[Data, Index], 0, len(state.Segments))

	for _, segment := range state.Segments {
		e := NewSeriesSegment[Data, Index](s.dataFactory, s.getIdx, s.idxCmp, s.areContinuous)
		e.Restore(segment)

		segments = append(segments, e)
	}

	s.segments = segments

	return nil
}

func (s *Series[Data, Index]) insertBeforeStart(periodStart, periodEnd Index, data []Data) error {
	newSegment := NewSeriesSegment[Data, Index](s.dataFactory, s.getIdx, s.idxCmp, s.areContinuous)

	if err := newSegment.MergePeriod(periodStart, periodEnd, data); err != nil {
		return err
	}

	s.segments = slices.Insert(s.segments, 0, newSegment)

	return nil
}

func (s *Series[Data, Index]) mergeWithStart(periodStart, periodEnd Index, data []Data, intersectLastSegmentIdx int) error {
	var newFirstSegment *SeriesSegment[Data, Index]

	intersectLastSegment := s.segments[intersectLastSegmentIdx]
	if intersectLastSegment.CanBeMergedWith(periodEnd) {
		if err := intersectLastSegment.MergePeriod(periodStart, periodEnd, data); err != nil {
			return err
		}

		newFirstSegment = intersectLastSegment
	} else {
		newFirstSegment = NewSeriesSegment(s.dataFactory, s.getIdx, s.idxCmp, s.areContinuous)

		if err := newFirstSegment.MergePeriod(periodStart, periodEnd, data); err != nil {
			return err
		}
	}

	s.segments = slices.Delete(s.segments, 0, intersectLastSegmentIdx)
	s.segments[0] = newFirstSegment

	return nil
}

func (s *Series[Data, Index]) insertAfterEnd(periodStart, periodEnd Index, data []Data) error {
	newSegment := NewSeriesSegment[Data, Index](s.dataFactory, s.getIdx, s.idxCmp, s.areContinuous)

	if err := newSegment.MergePeriod(periodStart, periodEnd, data); err != nil {
		return err
	}

	s.segments = append(s.segments, newSegment)

	return nil
}

func (s *Series[Data, Index]) mergeWithEnd(periodStart, periodEnd Index, data []Data, intersectFirstSegmentIdx int) error {
	lastSegmentsToDelete := len(s.segments) - intersectFirstSegmentIdx - 1

	intersectFirstSegment := s.segments[intersectFirstSegmentIdx]
	if intersectFirstSegment.CanBeMergedWith(periodStart) {
		if err := intersectFirstSegment.MergePeriod(periodStart, periodEnd, data); err != nil {
			return err
		}
	} else {
		lastSegmentsToDelete--

		newSegment := NewSeriesSegment(s.dataFactory, s.getIdx, s.idxCmp, s.areContinuous)

		if err := newSegment.MergePeriod(periodStart, periodEnd, data); err != nil {
			return err
		}

		s.segments[intersectFirstSegmentIdx+1] = newSegment
	}

	s.segments = slices.Delete(s.segments, len(s.segments)-lastSegmentsToDelete, len(s.segments))

	return nil
}

func (s *Series[Data, Index]) mergeWithinRange(periodStart, periodEnd Index, data []Data, intersectFirstSegmentIdx, intersectLastSegmentIdx int) error {
	firstSegment := s.segments[intersectFirstSegmentIdx]
	canBeMergedWithFirst := firstSegment.CanBeMergedWith(periodStart)

	if intersectFirstSegmentIdx == intersectLastSegmentIdx && canBeMergedWithFirst {
		return firstSegment.MergePeriod(periodStart, periodEnd, data)
	}

	lastSegment := s.segments[intersectLastSegmentIdx]
	canBeMergedWithLast := lastSegment.CanBeMergedWith(periodEnd)

	if canBeMergedWithFirst && canBeMergedWithLast {
		firstSegmentData, err := firstSegment.Data.GetEndOpen(firstSegment.PeriodStart, periodStart)
		if err != nil {
			return err
		}

		data = append(firstSegmentData, data...)
		periodStart = s.getSmallerIndex(firstSegment.PeriodStart, periodStart)

		if err := lastSegment.MergePeriod(periodStart, periodEnd, data); err != nil {
			return err
		}

		s.segments[intersectFirstSegmentIdx] = lastSegment

		deleteFrom := intersectFirstSegmentIdx + 1
		segmentsToDelete := intersectLastSegmentIdx - intersectFirstSegmentIdx
		s.segments = slices.Delete(s.segments, deleteFrom, deleteFrom+segmentsToDelete)

		return nil
	}

	//var resultingSegmentIdx int
	//segmentsToDelete := intersectLastSegmentIdx - intersectFirstSegmentIdx

	if canBeMergedWithFirst {
		if err := firstSegment.MergePeriod(periodStart, periodEnd, data); err != nil {
			return err
		}

		deleteFrom := intersectFirstSegmentIdx + 1
		segmentsToDelete := intersectLastSegmentIdx - intersectFirstSegmentIdx
		s.segments = slices.Delete(s.segments, deleteFrom, deleteFrom+segmentsToDelete)

		return nil
	}

	if canBeMergedWithLast {
		if err := lastSegment.MergePeriod(periodStart, periodEnd, data); err != nil {
			return err
		}

		deleteFrom := intersectFirstSegmentIdx + 1
		segmentsToDelete := intersectLastSegmentIdx - intersectFirstSegmentIdx - 1
		s.segments = slices.Delete(s.segments, deleteFrom, deleteFrom+segmentsToDelete)

		return nil
	}

	newSegment := NewSeriesSegment(s.dataFactory, s.getIdx, s.idxCmp, s.areContinuous)

	if err := newSegment.MergePeriod(periodStart, periodEnd, data); err != nil {
		return err
	}

	if intersectFirstSegmentIdx == intersectLastSegmentIdx {
		s.segments = slices.Insert(s.segments, intersectFirstSegmentIdx+1, newSegment)
		return nil
	}

	s.segments[intersectFirstSegmentIdx+1] = newSegment
	segmentsToDelete := intersectLastSegmentIdx - intersectFirstSegmentIdx - 1
	s.segments = slices.Delete(s.segments, intersectFirstSegmentIdx+2, intersectFirstSegmentIdx+2+segmentsToDelete)

	return nil
}

func (s *Series[Data, Index]) findSegmentWhichStartsBeforeOrAt(t Index, includeContinuous bool) (_ int, contains bool) { // PeriodStart >= t
	segmentWhichStartsLaterOrAt := sort.Search(len(s.segments), func(i int) bool {
		return s.idxCmp(s.segments[i].PeriodStart, t) >= 0
	})

	// if segmentWhichStartsLaterOrAt == 0 {
	// 	if s.cmp(s.segments[0].PeriodStart, t) > 0 && (!includeContinuous || !s.areContinuous(t, s.segments[0].PeriodStart)) {
	// 		return -1, false
	// 	}
	// 	return 0, true
	// }

	evenLastSegmentStartsBefore := segmentWhichStartsLaterOrAt == len(s.segments)

	if evenLastSegmentStartsBefore {
		lastSegment := segmentWhichStartsLaterOrAt - 1
		lastSegmentEnd := s.segments[lastSegment].PeriodEnd
		contains := s.idxCmp(t, lastSegmentEnd) <= 0 || (includeContinuous && s.areContinuous(lastSegmentEnd, t))
		return lastSegment, contains
	}

	segmentStartsLater := s.idxCmp(s.segments[segmentWhichStartsLaterOrAt].PeriodStart, t) > 0
	areContinuous := includeContinuous && s.areContinuous(t, s.segments[segmentWhichStartsLaterOrAt].PeriodStart)
	segmentStartsAt := !segmentStartsLater || areContinuous

	if segmentStartsAt {
		return segmentWhichStartsLaterOrAt, true
	}
	if segmentWhichStartsLaterOrAt == 0 && !areContinuous {
		return -1, false
	}

	segment := segmentWhichStartsLaterOrAt - 1
	segmentEnd := s.segments[segment].PeriodEnd
	contains = s.idxCmp(t, segmentEnd) <= 0 || (includeContinuous && s.areContinuous(segmentEnd, t))
	return segment, contains
}

func (s *Series[Data, Index]) getSmallerIndex(idx1, idx2 Index) Index {
	if s.idxCmp(idx1, idx2) < 0 {
		return idx1
	}

	return idx2
}

type SeriesState[Data any, Index any] struct {
	Segments []*SeriesSegmentFields[Data, Index]
}
