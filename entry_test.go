package sparse_test

import (
	"testing"

	"github.com/nnikolash/go-sparse"
	"github.com/stretchr/testify/require"
)

func TestSparseSeriesEntry_Merge1(t *testing.T) {
	t.Parallel()

	entry := sparse.NewSeriesEntry[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	p := entry.ContainsPoint(31)
	require.False(t, p)
	p = entry.ContainsPoint(32)
	require.False(t, p)
	p = entry.ContainsPoint(33)
	require.False(t, p)
	err := entry.MergePeriod(31, 33, nil)
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	p = entry.ContainsPoint(31)
	require.True(t, p)
	p = entry.ContainsPoint(32)
	require.True(t, p)
	p = entry.ContainsPoint(33)
	require.True(t, p)

	err = entry.MergePeriod(10, 32, []int{10, 20, 30})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(entry.GetAll()))
	s, e, d, err := entry.GetAllInRange(-100, 100)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, d)
	require.Equal(t, 10, s)
	require.Equal(t, 33, e)

	p = entry.ContainsPoint(10)
	require.True(t, p)
	p = entry.ContainsPoint(15)
	require.True(t, p)
	p = entry.ContainsPoint(20)
	require.True(t, p)
	p = entry.ContainsPoint(31)
	require.True(t, p)
	p = entry.ContainsPoint(32)
	require.True(t, p)
	p = entry.ContainsPoint(33)
	require.True(t, p)

	p = entry.ContainsPoint(8)
	require.False(t, p)
	p = entry.ContainsPoint(9)
	require.False(t, p)
	err = entry.MergePeriod(8, 12, []int{10})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(entry.GetAll()))
	p = entry.ContainsPoint(8)
	require.True(t, p)
	p = entry.ContainsPoint(9)
	require.True(t, p)

	err = entry.MergeData([]int{30, 40, 50})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 20, 30, 40, 50}, must2(entry.GetAll()))

	err = entry.MergeData([]int{-10, 0, 10})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{-10, 0, 10, 20, 30, 40, 50}, must2(entry.GetAll()))

	err = entry.MergeData([]int{10, 20})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{-10, 0, 10, 20, 30, 40, 50}, must2(entry.GetAll()))
	_, _, _, err = entry.GetAllInRange(-100, -70)
	require.Error(t, err)
	_, _, _, err = entry.GetAllInRange(70, 100)
	require.Error(t, err)

	err = entry.MergeData([]int{30, 40, 50, 60})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{-10, 0, 10, 20, 30, 40, 50, 60}, must2(entry.GetAll()))
	s, e, d, err = entry.GetAllInRange(0, 100)
	require.Equal(t, []int{0, 10, 20, 30, 40, 50, 60}, d)
	require.Equal(t, 0, s)
	require.Equal(t, 60, e)

	err = entry.MergeData([]int{40, 50, 60})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{-10, 0, 10, 20, 30, 40, 50, 60}, must2(entry.GetAll()))
	s, e, d, err = entry.GetAllInRange(-5, 35)
	require.NoError(t, err)
	require.Equal(t, []int{0, 10, 20, 30}, d)
	require.Equal(t, -5, s)
	require.Equal(t, 35, e)

	err = entry.MergeData([]int{-20, -10, 0, 10})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{-20, -10, 0, 10, 20, 30, 40, 50, 60}, must2(entry.GetAll()))
	s, e, d, err = entry.GetAllInRange(-100, 30)
	require.NoError(t, err)
	require.Equal(t, []int{-20, -10, 0, 10, 20, 30}, d)
	require.Equal(t, -20, s)
	require.Equal(t, 30, e)

	err = entry.MergeData([]int{-40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 80})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{-40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 80}, must2(entry.GetAll()))

	err = entry.MergeData([]int{-50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 80, 90})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{-50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 80, 90}, must2(entry.GetAll()))

	err = entry.MergeData([]int{90, 100})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{-50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100}, must2(entry.GetAll()))

	err = entry.MergePeriod(100, 111, []int{100, 110})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{-50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110}, must2(entry.GetAll()))
	p = entry.ContainsPoint(111)
	require.True(t, p)
}

func TestSparseSeriesEntry_MergeBigger(t *testing.T) {
	t.Parallel()

	entry := sparse.NewSeriesEntry[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := entry.MergeData([]int{10, 20})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 20}, must2(entry.GetAll()))
	require.Equal(t, 10, entry.PeriodStart)
	require.Equal(t, 20, entry.PeriodEnd)

	err = entry.MergeData([]int{5, 15, 25})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{5, 15, 25}, must2(entry.GetAll()))
	require.Equal(t, 5, entry.PeriodStart)
	require.Equal(t, 25, entry.PeriodEnd)
}

func TestSparseSeriesEntry_MergeSmaller(t *testing.T) {
	t.Parallel()

	entry := sparse.NewSeriesEntry[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := entry.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(entry.GetAll()))
	require.Equal(t, 10, entry.PeriodStart)
	require.Equal(t, 30, entry.PeriodEnd)

	err = entry.MergeData([]int{15, 25})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 15, 25, 30}, must2(entry.GetAll()))
	require.Equal(t, 10, entry.PeriodStart)
	require.Equal(t, 30, entry.PeriodEnd)
}

func TestSparseSeriesEntry_MergeStart(t *testing.T) {
	t.Parallel()

	entry := sparse.NewSeriesEntry[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := entry.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(entry.GetAll()))
	require.Equal(t, 10, entry.PeriodStart)
	require.Equal(t, 30, entry.PeriodEnd)

	err = entry.MergeData([]int{5, 15})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{5, 15, 20, 30}, must2(entry.GetAll()))
	require.Equal(t, 5, entry.PeriodStart)
	require.Equal(t, 30, entry.PeriodEnd)
}

func TestSparseSeriesEntry_MergeStartMatchedEnd(t *testing.T) {
	t.Parallel()

	entry := sparse.NewSeriesEntry[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := entry.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(entry.GetAll()))
	require.Equal(t, 10, entry.PeriodStart)
	require.Equal(t, 30, entry.PeriodEnd)

	err = entry.MergeData([]int{5, 15, 30})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{5, 15, 30}, must2(entry.GetAll()))
	require.Equal(t, 5, entry.PeriodStart)
	require.Equal(t, 30, entry.PeriodEnd)
}

func TestSparseSeriesEntry_MergeStartMatchedStart(t *testing.T) {
	t.Parallel()

	entry := sparse.NewSeriesEntry[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := entry.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(entry.GetAll()))
	require.Equal(t, 10, entry.PeriodStart)
	require.Equal(t, 30, entry.PeriodEnd)

	err = entry.MergeData([]int{5, 10})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{5, 10, 20, 30}, must2(entry.GetAll()))
	require.Equal(t, 5, entry.PeriodStart)
	require.Equal(t, 30, entry.PeriodEnd)
}

func TestSparseSeriesEntry_MergeStartContinuous(t *testing.T) {
	t.Parallel()

	entry := sparse.NewSeriesEntry[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		func(smaller, bigger int) bool { return bigger-smaller == 1 },
	)

	err := entry.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(entry.GetAll()))
	require.Equal(t, 10, entry.PeriodStart)
	require.Equal(t, 30, entry.PeriodEnd)

	err = entry.MergeData([]int{5, 9})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{5, 9, 10, 20, 30}, must2(entry.GetAll()))
	require.Equal(t, 5, entry.PeriodStart)
	require.Equal(t, 30, entry.PeriodEnd)
}

func TestSparseSeriesEntry_MergeEnd(t *testing.T) {
	t.Parallel()

	entry := sparse.NewSeriesEntry[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := entry.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(entry.GetAll()))
	require.Equal(t, 10, entry.PeriodStart)
	require.Equal(t, 30, entry.PeriodEnd)

	err = entry.MergeData([]int{25, 35})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 20, 25, 35}, must2(entry.GetAll()))
	require.Equal(t, 10, entry.PeriodStart)
	require.Equal(t, 35, entry.PeriodEnd)
}

func TestSparseSeriesEntry_MergeEndMatchedStart(t *testing.T) {
	t.Parallel()

	entry := sparse.NewSeriesEntry[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := entry.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(entry.GetAll()))
	require.Equal(t, 10, entry.PeriodStart)
	require.Equal(t, 30, entry.PeriodEnd)

	err = entry.MergeData([]int{10, 15, 35})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 15, 35}, must2(entry.GetAll()))
	require.Equal(t, 10, entry.PeriodStart)
	require.Equal(t, 35, entry.PeriodEnd)
}

func TestSparseSeriesEntry_MergeEndMatchedEnd(t *testing.T) {
	t.Parallel()

	entry := sparse.NewSeriesEntry[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := entry.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(entry.GetAll()))
	require.Equal(t, 10, entry.PeriodStart)
	require.Equal(t, 30, entry.PeriodEnd)

	err = entry.MergeData([]int{30, 35})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 20, 30, 35}, must2(entry.GetAll()))
	require.Equal(t, 10, entry.PeriodStart)
	require.Equal(t, 35, entry.PeriodEnd)
}

func TestSparseSeriesEntry_MergeEndContinuous(t *testing.T) {
	t.Parallel()

	entry := sparse.NewSeriesEntry[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		func(smaller, bigger int) bool { return bigger-smaller == 1 },
	)

	err := entry.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(entry.GetAll()))
	require.Equal(t, 10, entry.PeriodStart)
	require.Equal(t, 30, entry.PeriodEnd)

	err = entry.MergeData([]int{31, 35})
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, []int{10, 20, 30, 31, 35}, must2(entry.GetAll()))
	require.Equal(t, 10, entry.PeriodStart)
	require.Equal(t, 35, entry.PeriodEnd)
}

func TestSparseSeriesEntry_MergeDuplicate(t *testing.T) {
	t.Parallel()

	type Entry struct {
		IntIdx int
	}

	entry := sparse.NewSeriesEntry[Entry, int](
		sparse.NewArrayData,
		func(v *Entry) int { return v.IntIdx },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	makeEntries := func(data []int) []Entry {
		entries := make([]Entry, 0, 3*len(data))
		for _, v := range data {
			entries = append(entries, Entry{v})
			entries = append(entries, Entry{v})
			entries = append(entries, Entry{v})
			entries = append(entries, Entry{v})
		}
		return entries
	}

	err := entry.MergePeriod(9, 31, makeEntries([]int{10, 20, 30}))
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, makeEntries([]int{10, 20, 30}), must2(entry.GetAll()))

	err = entry.MergePeriod(29, 41, makeEntries([]int{30, 40}))
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, makeEntries([]int{10, 20, 30, 40}), must2(entry.GetAll()))

	err = entry.MergePeriod(-1, 11, makeEntries([]int{0, 10}))
	require.NoError(t, err)
	println(0, entry.Data, entry.PeriodStart, entry.PeriodEnd)
	require.Equal(t, makeEntries([]int{0, 10, 20, 30, 40}), must2(entry.GetAll()))

	// TODO: more tests on duplicates
}
