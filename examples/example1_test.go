package examples_test

import (
	"testing"
	"time"

	"github.com/nnikolash/go-sparse"
	"github.com/stretchr/testify/require"
)

type TestEvent struct {
	Time time.Time
	Data int
}

func TestExample1(t *testing.T) {
	t.Parallel()

	dataStorageFactory := sparse.NewArrayData[TestEvent, time.Time]

	getIdx := func(data *TestEvent) time.Time {
		return data.Time
	}

	cmp := func(idx1, idx2 time.Time) int {
		return idx1.Compare(idx2)
	}

	areContinuous := func(smaller, bigger time.Time) bool {
		return smaller.UnixNano() == bigger.UnixNano()+1
	}

	series := sparse.NewSeries(dataStorageFactory, getIdx, cmp, areContinuous)

	// Adding period [1; 3]
	series.AddData([]TestEvent{
		{Time: time.Unix(1, 0), Data: 1},
		{Time: time.Unix(2, 0), Data: 2},
		{Time: time.Unix(3, 0), Data: 3},
	})

	// Adding period [4; 6]
	series.AddData([]TestEvent{
		{Time: time.Unix(4, 0), Data: 4},
		{Time: time.Unix(5, 0), Data: 5},
		{Time: time.Unix(6, 0), Data: 6},
	})

	// Getting everything in range [1; 3), or [1; 2.000000005] if to be exact
	res, err := series.Get(time.Unix(1, 0), time.Unix(2, 5))
	require.NoError(t, err)
	require.Equal(t, []TestEvent{
		{Time: time.Unix(1, 0), Data: 1},
		{Time: time.Unix(2, 0), Data: 2},
	}, res)

	// Getting everything in range (1; 3)
	res, err = series.Get(time.Unix(1, 5), time.Unix(2, 5))
	require.NoError(t, err)
	require.Equal(t, []TestEvent{
		{Time: time.Unix(2, 0), Data: 2},
	}, res)

	// Notice there is an error if we try getting [2; 5], because (3; 4) is missing
	_, err = series.Get(time.Unix(2, 0), time.Unix(5, 0))
	require.Error(t, err)

	// Same for [3, 4]
	_, err = series.Get(time.Unix(3, 0), time.Unix(4, 0))
	require.Error(t, err)

	// Lets add [3; 4]
	series.AddData([]TestEvent{
		{Time: time.Unix(3, 0), Data: 3},
		{Time: time.Unix(4, 0), Data: 4},
	})

	// Now we can get [2; 5]
	res, err = series.Get(time.Unix(2, 0), time.Unix(5, 0))
	require.NoError(t, err)
	require.Equal(t, []TestEvent{
		{Time: time.Unix(2, 0), Data: 2},
		{Time: time.Unix(3, 0), Data: 3},
		{Time: time.Unix(4, 0), Data: 4},
		{Time: time.Unix(5, 0), Data: 5},
	}, res)

	// And [3; 4]
	res, err = series.Get(time.Unix(3, 0), time.Unix(4, 0))
	require.NoError(t, err)
	require.Equal(t, []TestEvent{
		{Time: time.Unix(3, 0), Data: 3},
		{Time: time.Unix(4, 0), Data: 4},
	}, res)
}
