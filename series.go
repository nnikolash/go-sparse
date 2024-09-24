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
	entries       []*SeriesEntry[Data, Index]
}

func (s *Series[Data, Index]) Entries() []*SeriesEntry[Data, Index] {
	return s.entries
}

// For debugging purposes
func (s *Series[Data, Index]) EntriesString() string {
	var res strings.Builder
	for _, entry := range s.entries {
		if res.Len() > 0 {
			res.WriteString("| ")
		}

		data, err := entry.Data.Get(entry.PeriodStart, entry.PeriodEnd)
		if err != nil {
			panic(fmt.Errorf("could not get data: %v", err))
		}

		for _, elem := range data {
			res.WriteString(fmt.Sprintf("%v ", s.getIdx(&elem)))
		}
	}

	return res.String()
}

func (s *Series[Data, Index]) GetAllEntries() []*SeriesEntry[Data, Index] {
	if len(s.entries) == 0 {
		return nil
	}

	return s.entries
}

func (s *Series[Data, Index]) GetEntry(t Index) *SeriesEntry[Data, Index] {
	entryIdx, contains := s.findEntryWhichStartsBeforeOrAt(t, false)
	if entryIdx == -1 && !contains {
		return nil
	}

	return s.entries[entryIdx]
}

func (s *Series[Data, Index]) Get(periodStart, periodEnd Index) ([]Data, error) {
	if len(s.entries) == 0 {
		return nil, errors.WithStack(&MissingPeriodError[Index]{PeriodStart: periodStart, PeriodEnd: periodEnd})
	}
	if s.idxCmp(periodStart, periodEnd) > 0 {
		return nil, errors.Errorf("requested period start is greater than period end: %v > %v", periodStart, periodEnd)
	}

	intersectLastEntryIdx, lastContains := s.findEntryWhichStartsBeforeOrAt(periodEnd, false)
	if intersectLastEntryIdx == -1 {
		return nil, errors.WithStack(&MissingPeriodError[Index]{PeriodStart: periodStart, PeriodEnd: periodEnd})
	}

	intersectFirstEntryIdx, firstContains := s.findEntryWhichStartsBeforeOrAt(periodStart, false)
	if intersectFirstEntryIdx == -1 {
		currentBeginning := s.entries[0].PeriodStart
		return nil, errors.WithStack(&MissingPeriodError[Index]{PeriodStart: periodStart, PeriodEnd: currentBeginning})
	}

	intersectFirstEntry := s.entries[intersectFirstEntryIdx]
	intersectLastEntry := s.entries[intersectLastEntryIdx]

	if intersectFirstEntryIdx != intersectLastEntryIdx || !firstContains || !lastContains {
		if firstContains {
			periodStart = intersectFirstEntry.PeriodEnd
		}
		if lastContains {
			periodEnd = intersectLastEntry.PeriodStart
		}
		return nil, errors.WithStack(&MissingPeriodError[Index]{PeriodStart: periodStart, PeriodEnd: periodEnd})
	}

	entry := intersectFirstEntry

	data, err := entry.Data.Get(periodStart, periodEnd)
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

func (s *Series[Data, Index]) GetPeriod(periodStart, periodEnd Index) *SeriesEntry[Data, Index] {
	if len(s.entries) == 0 {
		return nil
	}

	firstEntryIdx, contains := s.findEntryWhichStartsBeforeOrAt(periodStart, false)
	if firstEntryIdx == -1 || !contains {
		return nil
	}

	if s.idxCmp(periodStart, periodEnd) == 0 {
		return s.entries[firstEntryIdx]
	}

	lastEntryIdx, contains := s.findEntryWhichStartsBeforeOrAt(periodEnd, false)
	if lastEntryIdx == -1 || !contains {
		return nil
	}

	if firstEntryIdx != lastEntryIdx {
		return nil
	}

	return s.entries[firstEntryIdx]
}

func (s *Series[Data, Index]) GetPeriodClosestFromStart(t Index, nonEmpty bool) *SeriesEntry[Data, Index] {
	if len(s.entries) == 0 {
		return nil
	}

	entryIdx, _ := s.findEntryWhichStartsBeforeOrAt(t, false)
	if entryIdx == -1 {
		return nil
	}

	if nonEmpty {
		// TODO: this is ineffective... but I just hope there will be not much of empty entries
		for entryIdx >= 0 && s.entries[entryIdx].Empty {
			entryIdx--
		}
		if entryIdx == -1 {
			return nil
		}
	}

	return s.entries[entryIdx]
}

func (s *Series[Data, Index]) GetPeriodClosestFromEnd(t Index, nonEmpty bool) *SeriesEntry[Data, Index] {
	if len(s.entries) == 0 {
		return nil
	}

	entryIdx, contains := s.findEntryWhichStartsBeforeOrAt(t, false)
	if entryIdx == -1 {
		entryIdx = 0
	} else if !contains {
		if entryIdx == len(s.entries)-1 {
			return nil
		}
		entryIdx++
	}

	if nonEmpty {
		// TODO: this is ineffective... but I just hope there will be not much of empty entries
		for entryIdx < len(s.entries) && s.entries[entryIdx].Empty {
			entryIdx++
		}
		if entryIdx == len(s.entries) {
			return nil
		}
	}

	return s.entries[entryIdx]
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
	if len(s.entries) == 0 {
		newEntry := NewSeriesEntry[Data, Index](s.dataFactory, s.getIdx, s.idxCmp, s.areContinuous)

		if err := newEntry.MergePeriod(periodStart, periodEnd, data); err != nil {
			return err
		}

		s.entries = append(s.entries, newEntry)

		return nil
	}

	intersectFirstEntryIdx, firstContains := s.findEntryWhichStartsBeforeOrAt(periodStart, true)
	intersectLastEntryIdx, lastContains := s.findEntryWhichStartsBeforeOrAt(periodEnd, true)

	endsBeforeStart := intersectLastEntryIdx == -1
	if endsBeforeStart {
		return s.insertBeforeStart(periodStart, periodEnd, data)
	}

	startsBeforeStart := intersectFirstEntryIdx == -1
	if startsBeforeStart {
		return s.mergeWithStart(periodStart, periodEnd, data, intersectLastEntryIdx)
	}

	lastEntryIdx := len(s.entries) - 1

	if intersectFirstEntryIdx == lastEntryIdx && !firstContains {
		return s.insertAfterEnd(periodStart, periodEnd, data)
	}

	if intersectLastEntryIdx == lastEntryIdx && !lastContains {
		return s.mergeWithEnd(periodStart, periodEnd, data, intersectFirstEntryIdx)
	}

	return s.mergeWithinRange(periodStart, periodEnd, data, intersectFirstEntryIdx, intersectLastEntryIdx)
}

func (s *Series[Data, Index]) Restore(state *SeriesState[Data, Index]) error {
	for _, entry := range state.Entries {
		if s.idxCmp(entry.PeriodStart, entry.PeriodEnd) > 0 {
			return errors.Errorf("storage error: entry period start is greater than period end: %v > %v", entry.PeriodStart, entry.PeriodEnd)
		}
	}

	entries := make([]*SeriesEntry[Data, Index], 0, len(state.Entries))

	for _, entry := range state.Entries {
		e := NewSeriesEntry[Data, Index](s.dataFactory, s.getIdx, s.idxCmp, s.areContinuous)
		e.Restore(entry)

		entries = append(entries, e)
	}

	s.entries = entries

	return nil
}

func (s *Series[Data, Index]) insertBeforeStart(periodStart, periodEnd Index, data []Data) error {
	newEntry := NewSeriesEntry[Data, Index](s.dataFactory, s.getIdx, s.idxCmp, s.areContinuous)

	if err := newEntry.MergePeriod(periodStart, periodEnd, data); err != nil {
		return err
	}

	s.entries = slices.Insert(s.entries, 0, newEntry)

	return nil
}

func (s *Series[Data, Index]) mergeWithStart(periodStart, periodEnd Index, data []Data, intersectLastEntryIdx int) error {
	var newFirstEntry *SeriesEntry[Data, Index]

	intersectLastEntry := s.entries[intersectLastEntryIdx]
	if intersectLastEntry.CanBeMergedWith(periodEnd) {
		if err := intersectLastEntry.MergePeriod(periodStart, periodEnd, data); err != nil {
			return err
		}

		newFirstEntry = intersectLastEntry
	} else {
		newFirstEntry = NewSeriesEntry(s.dataFactory, s.getIdx, s.idxCmp, s.areContinuous)

		if err := newFirstEntry.MergePeriod(periodStart, periodEnd, data); err != nil {
			return err
		}
	}

	s.entries = slices.Delete(s.entries, 0, intersectLastEntryIdx)
	s.entries[0] = newFirstEntry

	return nil
}

func (s *Series[Data, Index]) insertAfterEnd(periodStart, periodEnd Index, data []Data) error {
	newEntry := NewSeriesEntry[Data, Index](s.dataFactory, s.getIdx, s.idxCmp, s.areContinuous)

	if err := newEntry.MergePeriod(periodStart, periodEnd, data); err != nil {
		return err
	}

	s.entries = append(s.entries, newEntry)

	return nil
}

func (s *Series[Data, Index]) mergeWithEnd(periodStart, periodEnd Index, data []Data, intersectFirstEntryIdx int) error {
	lastEntriesToDelete := len(s.entries) - intersectFirstEntryIdx - 1

	intersectFirstEntry := s.entries[intersectFirstEntryIdx]
	if intersectFirstEntry.CanBeMergedWith(periodStart) {
		if err := intersectFirstEntry.MergePeriod(periodStart, periodEnd, data); err != nil {
			return err
		}
	} else {
		lastEntriesToDelete--

		newEntry := NewSeriesEntry(s.dataFactory, s.getIdx, s.idxCmp, s.areContinuous)

		if err := newEntry.MergePeriod(periodStart, periodEnd, data); err != nil {
			return err
		}

		s.entries[intersectFirstEntryIdx+1] = newEntry
	}

	s.entries = slices.Delete(s.entries, len(s.entries)-lastEntriesToDelete, len(s.entries))

	return nil
}

func (s *Series[Data, Index]) mergeWithinRange(periodStart, periodEnd Index, data []Data, intersectFirstEntryIdx, intersectLastEntryIdx int) error {
	firstEntry := s.entries[intersectFirstEntryIdx]
	canBeMergedWithFirst := firstEntry.CanBeMergedWith(periodStart)

	if intersectFirstEntryIdx == intersectLastEntryIdx && canBeMergedWithFirst {
		return firstEntry.MergePeriod(periodStart, periodEnd, data)
	}

	lastEntry := s.entries[intersectLastEntryIdx]
	canBeMergedWithLast := lastEntry.CanBeMergedWith(periodEnd)

	if canBeMergedWithFirst && canBeMergedWithLast {
		firstEntryData, err := firstEntry.Data.GetEndOpen(firstEntry.PeriodStart, periodStart)
		if err != nil {
			return err
		}

		data = append(firstEntryData, data...)
		periodStart = s.getSmallerIndex(firstEntry.PeriodStart, periodStart)

		if err := lastEntry.MergePeriod(periodStart, periodEnd, data); err != nil {
			return err
		}

		s.entries[intersectFirstEntryIdx] = lastEntry

		deleteFrom := intersectFirstEntryIdx + 1
		entriesToDelete := intersectLastEntryIdx - intersectFirstEntryIdx
		s.entries = slices.Delete(s.entries, deleteFrom, deleteFrom+entriesToDelete)

		return nil
	}

	//var resultingEntryIdx int
	//entriesToDelete := intersectLastEntryIdx - intersectFirstEntryIdx

	if canBeMergedWithFirst {
		if err := firstEntry.MergePeriod(periodStart, periodEnd, data); err != nil {
			return err
		}

		deleteFrom := intersectFirstEntryIdx + 1
		entriesToDelete := intersectLastEntryIdx - intersectFirstEntryIdx
		s.entries = slices.Delete(s.entries, deleteFrom, deleteFrom+entriesToDelete)

		return nil
	}

	if canBeMergedWithLast {
		if err := lastEntry.MergePeriod(periodStart, periodEnd, data); err != nil {
			return err
		}

		deleteFrom := intersectFirstEntryIdx + 1
		entriesToDelete := intersectLastEntryIdx - intersectFirstEntryIdx - 1
		s.entries = slices.Delete(s.entries, deleteFrom, deleteFrom+entriesToDelete)

		return nil
	}

	newEntry := NewSeriesEntry(s.dataFactory, s.getIdx, s.idxCmp, s.areContinuous)

	if err := newEntry.MergePeriod(periodStart, periodEnd, data); err != nil {
		return err
	}

	if intersectFirstEntryIdx == intersectLastEntryIdx {
		s.entries = slices.Insert(s.entries, intersectFirstEntryIdx+1, newEntry)
		return nil
	}

	s.entries[intersectFirstEntryIdx+1] = newEntry
	entriesToDelete := intersectLastEntryIdx - intersectFirstEntryIdx - 1
	s.entries = slices.Delete(s.entries, intersectFirstEntryIdx+2, intersectFirstEntryIdx+2+entriesToDelete)

	return nil
}

func (s *Series[Data, Index]) findEntryWhichStartsBeforeOrAt(t Index, includeContinuous bool) (_ int, contains bool) { // PeriodStart >= t
	entryWhichStartsLaterOrAt := sort.Search(len(s.entries), func(i int) bool {
		return s.idxCmp(s.entries[i].PeriodStart, t) >= 0
	})

	// if entryWhichStartsLaterOrAt == 0 {
	// 	if s.cmp(s.entries[0].PeriodStart, t) > 0 && (!includeContinuous || !s.areContinuous(t, s.entries[0].PeriodStart)) {
	// 		return -1, false
	// 	}
	// 	return 0, true
	// }

	evenLastEntryStartsBefore := entryWhichStartsLaterOrAt == len(s.entries)

	if evenLastEntryStartsBefore {
		lastEntry := entryWhichStartsLaterOrAt - 1
		lastEntryEnd := s.entries[lastEntry].PeriodEnd
		contains := s.idxCmp(t, lastEntryEnd) <= 0 || (includeContinuous && s.areContinuous(lastEntryEnd, t))
		return lastEntry, contains
	}

	entryStartsLater := s.idxCmp(s.entries[entryWhichStartsLaterOrAt].PeriodStart, t) > 0
	areContinuous := includeContinuous && s.areContinuous(t, s.entries[entryWhichStartsLaterOrAt].PeriodStart)
	entryStartsAt := !entryStartsLater || areContinuous

	if entryStartsAt {
		return entryWhichStartsLaterOrAt, true
	}
	if entryWhichStartsLaterOrAt == 0 && !areContinuous {
		return -1, false
	}

	entry := entryWhichStartsLaterOrAt - 1
	entryEnd := s.entries[entry].PeriodEnd
	contains = s.idxCmp(t, entryEnd) <= 0 || (includeContinuous && s.areContinuous(entryEnd, t))
	return entry, contains
}

func (s *Series[Data, Index]) getSmallerIndex(idx1, idx2 Index) Index {
	if s.idxCmp(idx1, idx2) < 0 {
		return idx1
	}

	return idx2
}

type SeriesState[Data any, Index any] struct {
	Entries []*SeriesEntryFields[Data, Index]
}
