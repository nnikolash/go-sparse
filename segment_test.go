package sparse_test

import (
	"testing"

	"github.com/nnikolash/go-sparse"
	"github.com/stretchr/testify/require"
)

func TestSeriesSegment_Merge1(t *testing.T) {
	t.Parallel()

	segment := sparse.NewSeriesSegment[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	p := segment.ContainsPoint(31)
	require.False(t, p)
	p = segment.ContainsPoint(32)
	require.False(t, p)
	p = segment.ContainsPoint(33)
	require.False(t, p)
	err := segment.MergePeriod(31, 33, nil)
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	p = segment.ContainsPoint(31)
	require.True(t, p)
	p = segment.ContainsPoint(32)
	require.True(t, p)
	p = segment.ContainsPoint(33)
	require.True(t, p)

	err = segment.MergePeriod(10, 32, []int{10, 20, 30})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(segment.GetAll()))
	s, e, d, err := segment.GetAllInRange(-100, 100)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, d)
	require.Equal(t, 10, s)
	require.Equal(t, 33, e)

	p = segment.ContainsPoint(10)
	require.True(t, p)
	p = segment.ContainsPoint(15)
	require.True(t, p)
	p = segment.ContainsPoint(20)
	require.True(t, p)
	p = segment.ContainsPoint(31)
	require.True(t, p)
	p = segment.ContainsPoint(32)
	require.True(t, p)
	p = segment.ContainsPoint(33)
	require.True(t, p)

	p = segment.ContainsPoint(8)
	require.False(t, p)
	p = segment.ContainsPoint(9)
	require.False(t, p)
	err = segment.MergePeriod(8, 12, []int{10})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(segment.GetAll()))
	p = segment.ContainsPoint(8)
	require.True(t, p)
	p = segment.ContainsPoint(9)
	require.True(t, p)

	err = segment.MergeData([]int{30, 40, 50})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 20, 30, 40, 50}, must2(segment.GetAll()))

	err = segment.MergeData([]int{-10, 0, 10})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{-10, 0, 10, 20, 30, 40, 50}, must2(segment.GetAll()))

	err = segment.MergeData([]int{10, 20})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{-10, 0, 10, 20, 30, 40, 50}, must2(segment.GetAll()))
	_, _, _, err = segment.GetAllInRange(-100, -70)
	require.Error(t, err)
	_, _, _, err = segment.GetAllInRange(70, 100)
	require.Error(t, err)

	err = segment.MergeData([]int{30, 40, 50, 60})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{-10, 0, 10, 20, 30, 40, 50, 60}, must2(segment.GetAll()))
	s, e, d, err = segment.GetAllInRange(0, 100)
	require.NoError(t, err)
	require.Equal(t, []int{0, 10, 20, 30, 40, 50, 60}, d)
	require.Equal(t, 0, s)
	require.Equal(t, 60, e)

	err = segment.MergeData([]int{40, 50, 60})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{-10, 0, 10, 20, 30, 40, 50, 60}, must2(segment.GetAll()))
	s, e, d, err = segment.GetAllInRange(-5, 35)
	require.NoError(t, err)
	require.Equal(t, []int{0, 10, 20, 30}, d)
	require.Equal(t, -5, s)
	require.Equal(t, 35, e)

	err = segment.MergeData([]int{-20, -10, 0, 10})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{-20, -10, 0, 10, 20, 30, 40, 50, 60}, must2(segment.GetAll()))
	s, e, d, err = segment.GetAllInRange(-100, 30)
	require.NoError(t, err)
	require.Equal(t, []int{-20, -10, 0, 10, 20, 30}, d)
	require.Equal(t, -20, s)
	require.Equal(t, 30, e)

	err = segment.MergeData([]int{-40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 80})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{-40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 80}, must2(segment.GetAll()))

	err = segment.MergeData([]int{-50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 80, 90})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{-50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 80, 90}, must2(segment.GetAll()))

	err = segment.MergeData([]int{90, 100})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{-50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100}, must2(segment.GetAll()))

	err = segment.MergePeriod(100, 111, []int{100, 110})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{-50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110}, must2(segment.GetAll()))
	p = segment.ContainsPoint(111)
	require.True(t, p)
}

func TestSeriesSegment_MergeBigger(t *testing.T) {
	t.Parallel()

	segment := sparse.NewSeriesSegment[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := segment.MergeData([]int{10, 20})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 20}, must2(segment.GetAll()))
	require.Equal(t, 10, segment.PeriodStart)
	require.Equal(t, 20, segment.PeriodEnd)

	err = segment.MergeData([]int{5, 15, 25})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{5, 15, 25}, must2(segment.GetAll()))
	require.Equal(t, 5, segment.PeriodStart)
	require.Equal(t, 25, segment.PeriodEnd)
}

func TestSeriesSegment_MergeSmaller(t *testing.T) {
	t.Parallel()

	segment := sparse.NewSeriesSegment[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := segment.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(segment.GetAll()))
	require.Equal(t, 10, segment.PeriodStart)
	require.Equal(t, 30, segment.PeriodEnd)

	err = segment.MergeData([]int{15, 25})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 15, 25, 30}, must2(segment.GetAll()))
	require.Equal(t, 10, segment.PeriodStart)
	require.Equal(t, 30, segment.PeriodEnd)
}

func TestSeriesSegment_MergeStart(t *testing.T) {
	t.Parallel()

	segment := sparse.NewSeriesSegment[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := segment.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(segment.GetAll()))
	require.Equal(t, 10, segment.PeriodStart)
	require.Equal(t, 30, segment.PeriodEnd)

	err = segment.MergeData([]int{5, 15})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{5, 15, 20, 30}, must2(segment.GetAll()))
	require.Equal(t, 5, segment.PeriodStart)
	require.Equal(t, 30, segment.PeriodEnd)
}

func TestSeriesSegment_MergeStartMatchedEnd(t *testing.T) {
	t.Parallel()

	segment := sparse.NewSeriesSegment[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := segment.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(segment.GetAll()))
	require.Equal(t, 10, segment.PeriodStart)
	require.Equal(t, 30, segment.PeriodEnd)

	err = segment.MergeData([]int{5, 15, 30})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{5, 15, 30}, must2(segment.GetAll()))
	require.Equal(t, 5, segment.PeriodStart)
	require.Equal(t, 30, segment.PeriodEnd)
}

func TestSeriesSegment_MergeStartMatchedStart(t *testing.T) {
	t.Parallel()

	segment := sparse.NewSeriesSegment[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := segment.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(segment.GetAll()))
	require.Equal(t, 10, segment.PeriodStart)
	require.Equal(t, 30, segment.PeriodEnd)

	err = segment.MergeData([]int{5, 10})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{5, 10, 20, 30}, must2(segment.GetAll()))
	require.Equal(t, 5, segment.PeriodStart)
	require.Equal(t, 30, segment.PeriodEnd)
}

func TestSeriesSegment_MergeStartContinuous(t *testing.T) {
	t.Parallel()

	segment := sparse.NewSeriesSegment[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		func(smaller, bigger int) bool { return bigger-smaller == 1 },
	)

	err := segment.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(segment.GetAll()))
	require.Equal(t, 10, segment.PeriodStart)
	require.Equal(t, 30, segment.PeriodEnd)

	err = segment.MergeData([]int{5, 9})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{5, 9, 10, 20, 30}, must2(segment.GetAll()))
	require.Equal(t, 5, segment.PeriodStart)
	require.Equal(t, 30, segment.PeriodEnd)
}

func TestSeriesSegment_MergeEnd(t *testing.T) {
	t.Parallel()

	segment := sparse.NewSeriesSegment[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := segment.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(segment.GetAll()))
	require.Equal(t, 10, segment.PeriodStart)
	require.Equal(t, 30, segment.PeriodEnd)

	err = segment.MergeData([]int{25, 35})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 20, 25, 35}, must2(segment.GetAll()))
	require.Equal(t, 10, segment.PeriodStart)
	require.Equal(t, 35, segment.PeriodEnd)
}

func TestSeriesSegment_MergeEndMatchedStart(t *testing.T) {
	t.Parallel()

	segment := sparse.NewSeriesSegment[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := segment.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(segment.GetAll()))
	require.Equal(t, 10, segment.PeriodStart)
	require.Equal(t, 30, segment.PeriodEnd)

	err = segment.MergeData([]int{10, 15, 35})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 15, 35}, must2(segment.GetAll()))
	require.Equal(t, 10, segment.PeriodStart)
	require.Equal(t, 35, segment.PeriodEnd)
}

func TestSeriesSegment_MergeEndMatchedEnd(t *testing.T) {
	t.Parallel()

	segment := sparse.NewSeriesSegment[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := segment.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(segment.GetAll()))
	require.Equal(t, 10, segment.PeriodStart)
	require.Equal(t, 30, segment.PeriodEnd)

	err = segment.MergeData([]int{30, 35})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 20, 30, 35}, must2(segment.GetAll()))
	require.Equal(t, 10, segment.PeriodStart)
	require.Equal(t, 35, segment.PeriodEnd)
}

func TestSeriesSegment_MergeEndContinuous(t *testing.T) {
	t.Parallel()

	segment := sparse.NewSeriesSegment[int, int](
		sparse.NewArrayData,
		func(v *int) int { return *v },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		func(smaller, bigger int) bool { return bigger-smaller == 1 },
	)

	err := segment.MergeData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 20, 30}, must2(segment.GetAll()))
	require.Equal(t, 10, segment.PeriodStart)
	require.Equal(t, 30, segment.PeriodEnd)

	err = segment.MergeData([]int{31, 35})
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, []int{10, 20, 30, 31, 35}, must2(segment.GetAll()))
	require.Equal(t, 10, segment.PeriodStart)
	require.Equal(t, 35, segment.PeriodEnd)
}

func TestSeriesSegment_MergeDuplicate(t *testing.T) {
	t.Parallel()

	type Data struct {
		IntIdx int
	}

	segment := sparse.NewSeriesSegment[Data, int](
		sparse.NewArrayData,
		func(v *Data) int { return v.IntIdx },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	makeEntries := func(data []int) []Data {
		entries := make([]Data, 0, 3*len(data))
		for _, v := range data {
			entries = append(entries, Data{v})
			entries = append(entries, Data{v})
			entries = append(entries, Data{v})
			entries = append(entries, Data{v})
		}
		return entries
	}

	err := segment.MergePeriod(9, 31, makeEntries([]int{10, 20, 30}))
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, makeEntries([]int{10, 20, 30}), must2(segment.GetAll()))

	err = segment.MergePeriod(29, 41, makeEntries([]int{30, 40}))
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, makeEntries([]int{10, 20, 30, 40}), must2(segment.GetAll()))

	err = segment.MergePeriod(-1, 11, makeEntries([]int{0, 10}))
	require.NoError(t, err)
	println(0, segment.Data, segment.PeriodStart, segment.PeriodEnd)
	require.Equal(t, makeEntries([]int{0, 10, 20, 30, 40}), must2(segment.GetAll()))

	// TODO: more tests on duplicates
}
