package sparse_test

import (
	"fmt"
	"testing"

	"github.com/nnikolash/go-sparse"
	"github.com/stretchr/testify/require"
)

func intSparseSeries() *sparse.Series[int, int] {
	return sparse.NewSeries[int, int](
		sparse.NewArrayData,
		func(data *int) int { return *data },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		func(smaller, bigger int) bool { return bigger-smaller == 1 },
	)
}

func TestSparseSeries_SimpleMerge(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	duplicate := func(data []int) []int {
		if data == nil {
			return nil
		}

		res := make([]int, 0, 2*len(data))
		for _, v := range data {
			res = append(res, v, v)
		}
		return res
	}

	var verifyInitialAdd func(t *testing.T, series *sparse.Series[int, int])
	var verifyAddEmptyRight1 func(t *testing.T, series *sparse.Series[int, int])
	var verifyAddEmptyRight2 func(t *testing.T, series *sparse.Series[int, int])
	var verifyAddEmptyLeft1 func(t *testing.T, series *sparse.Series[int, int])
	var verifyAddEmptyLeft2 func(t *testing.T, series *sparse.Series[int, int])
	var verifyMergeEnd func(t *testing.T, series *sparse.Series[int, int])
	var verifyMergeStart func(t *testing.T, series *sparse.Series[int, int])
	var verifyMergeInRangeDifferent func(t *testing.T, series *sparse.Series[int, int])
	var verifyMergeInRangeSame func(t *testing.T, series *sparse.Series[int, int])

	var emptyRightAdded1 = false
	var emptyLeftAdded1 = false
	var modified = false

	if ok := t.Run("InitialAdd", func(t *testing.T) {
		err := series.AddData(duplicate([]int{20}))
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SimpleMerge", 0, series.EntriesString())

		verifyInitialAdd = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(20, 20)
			require.NoError(t, err)
			if !modified {
				require.Equal(t, duplicate([]int{20}), res)
			} else {
				require.Equal(t, duplicate([]int(nil)), res)
			}

			emptyRightCheck := require.Error
			if emptyRightAdded1 {
				emptyRightCheck = require.NoError
			}

			_, err = series.Get(20, 25)
			emptyRightCheck(t, err)

			emptyLeftCheck := require.Error
			if emptyLeftAdded1 {
				emptyLeftCheck = require.NoError
			}

			_, err = series.Get(15, 20)
			emptyLeftCheck(t, err)
		}
		verifyInitialAdd(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("AddEmptyRight1", func(t *testing.T) {
		err := series.AddPeriod(20, 25, duplicate([]int{20}))
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SimpleMerge", 0, series.EntriesString())
		emptyRightAdded1 = true

		verifyAddEmptyRight1 = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(20, 25)
			require.NoError(t, err)
			if !modified {
				require.Equal(t, duplicate([]int{20}), res)
			} else {
				require.Equal(t, duplicate([]int{25}), res)
			}

			verifyInitialAdd(t, series)
		}
		verifyAddEmptyRight1(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("AddEmptyRight2", func(t *testing.T) {
		err := series.AddPeriod(25, 30, duplicate([]int{30}))
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SimpleMerge", 0, series.EntriesString())

		verifyAddEmptyRight2 = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(20, 30)
			require.NoError(t, err)
			if !modified {
				require.Equal(t, duplicate([]int{20, 30}), res)
			} else {
				require.Equal(t, duplicate([]int{25, 30}), res)
			}

			verifyAddEmptyRight1(t, series)
		}
		verifyAddEmptyRight2(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("AddEmptyLeft1", func(t *testing.T) {
		err := series.AddPeriod(15, 20, duplicate([]int{20}))
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SimpleMerge", 0, series.EntriesString())
		emptyLeftAdded1 = true

		verifyAddEmptyLeft1 = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(15, 20)
			require.NoError(t, err)
			if !modified {
				require.Equal(t, duplicate([]int{20}), res)
			} else {
				require.Equal(t, duplicate([]int{15}), res)
			}

			verifyAddEmptyRight2(t, series)
		}
		verifyAddEmptyLeft1(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("AddEmptyLeft2", func(t *testing.T) {
		err := series.AddPeriod(10, 15, duplicate([]int{10}))
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SimpleMerge", 0, series.EntriesString())

		verifyAddEmptyLeft2 = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(10, 30)
			require.NoError(t, err)
			if !modified {
				require.Equal(t, duplicate([]int{10, 20, 30}), res)
			} else {
				require.Equal(t, duplicate([]int{15, 25, 30}), res)
			}

			verifyAddEmptyLeft1(t, series)
		}
		verifyAddEmptyLeft2(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("MergeEnd", func(t *testing.T) {
		err := series.AddData(duplicate([]int{30, 40, 50}))
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SimpleMerge", 0, series.EntriesString())

		verifyMergeEnd = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(30, 50)
			require.NoError(t, err)
			require.Equal(t, duplicate([]int{30, 40, 50}), res)
			res, err = series.Get(10, 50)
			require.NoError(t, err)
			if !modified {
				require.Equal(t, duplicate([]int{10, 20, 30, 40, 50}), res)
			} else {
				require.Equal(t, duplicate([]int{15, 25, 30, 40, 50}), res)
			}
			verifyAddEmptyLeft2(t, series)
		}
		verifyMergeEnd(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("MergeStart", func(t *testing.T) {
		err := series.AddData(duplicate([]int{-10, 0, 10}))
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SimpleMerge", 0, series.EntriesString())

		verifyMergeStart = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(-10, 10)
			require.NoError(t, err)
			if !modified {
				require.Equal(t, duplicate([]int{-10, 0, 10}), res)
			} else {
				require.Equal(t, duplicate([]int{-10, 0, 5}), res)
			}
			res, err = series.Get(-10, 50)
			require.NoError(t, err)
			if !modified {
				require.Equal(t, duplicate([]int{-10, 0, 10, 20, 30, 40, 50}), res)
			} else {
				require.Equal(t, duplicate([]int{-10, 0, 5, 15, 25, 30, 40, 50}), res)
			}
			verifyMergeEnd(t, series)
		}
		verifyMergeStart(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("MergeInRangeDifferent", func(t *testing.T) {
		err := series.AddData(duplicate([]int{5, 15, 25}))
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SimpleMerge", 0, series.EntriesString())
		modified = true

		verifyMergeInRangeDifferent = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(5, 25)
			require.NoError(t, err)
			require.Equal(t, duplicate([]int{5, 15, 25}), res)
			res, err = series.Get(-10, 50)
			require.NoError(t, err)
			require.Equal(t, duplicate([]int{-10, 0, 5, 15, 25, 30, 40, 50}), res)
			verifyMergeStart(t, series)
		}
		verifyMergeInRangeDifferent(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("MergeInRangeSame", func(t *testing.T) {
		//err := series.AddData(duplicate([]int{30, 35, 40, 45, 50}))
		err := series.AddData(duplicate([]int{30, 40, 50}))
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SimpleMerge", 0, series.EntriesString())

		verifyMergeInRangeSame = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(30, 50)
			require.NoError(t, err)
			require.Equal(t, duplicate([]int{30, 40, 50}), res)
			res, err = series.Get(-10, 50)
			require.NoError(t, err)
			if !modified {
				require.Equal(t, duplicate([]int{-10, 0, 10, 20, 30, 40, 50}), res)
			} else {
				require.Equal(t, duplicate([]int{-10, 0, 5, 15, 25, 30, 40, 50}), res)
			}
			verifyMergeInRangeDifferent(t, series)
		}
		verifyMergeInRangeSame(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("GetOne", func(t *testing.T) {
		res, err := series.Get(30, 30)
		require.NoError(t, err)
		require.Equal(t, duplicate([]int{30}), res)

		res, err = series.Get(29, 31)
		require.NoError(t, err)
		require.Equal(t, duplicate([]int{30}), res)

		res, err = series.Get(30, 31)
		require.NoError(t, err)
		require.Equal(t, duplicate([]int{30}), res)

		res, err = series.Get(29, 30)
		require.NoError(t, err)
		require.Equal(t, duplicate([]int{30}), res)

		verifyMergeInRangeSame(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("GetNone", func(t *testing.T) {
		res, err := series.Get(31, 31)
		require.NoError(t, err)
		require.Empty(t, res)

		res, err = series.Get(29, 29)
		require.NoError(t, err)
		require.Empty(t, res)

		res, err = series.Get(31, 32)
		require.NoError(t, err)
		require.Empty(t, res)

		verifyMergeInRangeSame(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("GetError", func(t *testing.T) {
		_, err := series.Get(-30, -20)
		require.Error(t, err)

		_, err = series.Get(-30, -30)
		require.Error(t, err)

		_, err = series.Get(60, 70)
		require.Error(t, err)

		_, err = series.Get(60, 60)
		require.Error(t, err)

		verifyMergeInRangeSame(t, series)
	}); !ok {
		return
	}
}

func println(skip int, args ...interface{}) {
	callerPrintln(currentCallerFuncShortName(skip+1), skip+1, args...)
}

func callerPrintln(caller string, skip int, args ...interface{}) {
	args = append([]interface{}{caller, ":", currentCallerCodeLineNum(skip + 1), ":"}, args...)
	fmt.Println(args...)
}

func TestSparseSeries_SimpleSparseMergeInMiddle(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	println(0, series.EntriesString())

	err := series.AddData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, res)

	err = series.AddData([]int{40, 50, 60})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(40, 60)
	require.NoError(t, err)
	require.Equal(t, []int{40, 50, 60}, res)

	err = series.AddData([]int{70, 80, 90})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(70, 90)
	require.NoError(t, err)
	require.Equal(t, []int{70, 80, 90}, res)

	_, err = series.Get(30, 40)
	require.Error(t, err)
	_, err = series.Get(60, 70)
	require.Error(t, err)
	_, err = series.Get(30, 90)
	require.Error(t, err)

	err = series.AddData([]int{30, 31, 32})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 32)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30, 31, 32}, res)
	_, err = series.Get(30, 40)
	require.Error(t, err)

	err = series.AddData([]int{38, 39, 40})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(38, 60)
	require.NoError(t, err)
	require.Equal(t, []int{38, 39, 40, 50, 60}, res)
	_, err = series.Get(30, 40)
	require.Error(t, err)

	err = series.AddData([]int{60, 61, 62})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(40, 62)
	require.NoError(t, err)
	require.Equal(t, []int{40, 50, 60, 61, 62}, res)
	_, err = series.Get(60, 70)
	require.Error(t, err)

	err = series.AddData([]int{68, 69, 70})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(68, 90)
	require.NoError(t, err)
	require.Equal(t, []int{68, 69, 70, 80, 90}, res)
	_, err = series.Get(60, 70)
	require.Error(t, err)

	err = series.AddData([]int{32, 35, 38})
	require.NoError(t, err)
	println(0, series.EntriesString())
	err = series.AddData([]int{62, 65, 68})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 90)
	require.NoError(t, err, series.EntriesString())
	require.Equal(t, []int{10, 20, 30, 31, 32, 35, 38, 39, 40, 50, 60, 61, 62, 65, 68, 69, 70, 80, 90}, res, series.EntriesString())
}

func TestSparseSeries_SimpleSparseMergeInMiddleMultipleHoles(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	err := series.AddData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, res)

	err = series.AddData([]int{40, 50, 60})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(40, 60)
	require.NoError(t, err)
	require.Equal(t, []int{40, 50, 60}, res)

	err = series.AddData([]int{70, 80, 90})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(70, 90)
	require.NoError(t, err)
	require.Equal(t, []int{70, 80, 90}, res)

	err = series.AddData([]int{20, 30, 40, 50, 60, 70, 80})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 90)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30, 40, 50, 60, 70, 80, 90}, res)
}

func TestSparseSeries_SimpleSparseMergeInMiddleMultipleHolesOuside(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	err := series.AddData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, res)

	err = series.AddData([]int{50, 60})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(50, 60)
	require.NoError(t, err)
	require.Equal(t, []int{50, 60}, res)

	err = series.AddData([]int{80, 90})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(80, 90)
	require.NoError(t, err)
	require.Equal(t, []int{80, 90}, res)

	err = series.AddData([]int{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 90)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30, 40, 50, 60, 70, 80, 90}, res)
	res, err = series.Get(0, 100)
	require.NoError(t, err)
	require.Equal(t, []int{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100}, res)
}

func TestSparseSeries_SimpleSparseMergeInRangeInHole(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	err := series.AddData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, res)

	err = series.AddData([]int{80, 90})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(80, 90)
	require.NoError(t, err)
	require.Equal(t, []int{80, 90}, res)

	err = series.AddData([]int{50, 60})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(50, 60)
	require.NoError(t, err)
	require.Equal(t, []int{50, 60}, res)

	err = series.AddData([]int{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 90)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30, 40, 50, 60, 70, 80, 90}, res)
	res, err = series.Get(0, 100)
	require.NoError(t, err)
	require.Equal(t, []int{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100}, res)
}

func TestSparseSeries_SparseMerge(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	err := series.AddData([]int{10, 20, 30})
	require.NoError(t, err)

	var verifyAddEnd func(t *testing.T, series *sparse.Series[int, int])
	var verifyAddEndOne func(t *testing.T, series *sparse.Series[int, int])
	var verifyAddStart func(t *testing.T, series *sparse.Series[int, int])
	var verifyAddStartOne func(t *testing.T, series *sparse.Series[int, int])
	var verifyMergeToInitial func(t *testing.T, series *sparse.Series[int, int])
	var verifyCoverLeftHole1 func(t *testing.T, series *sparse.Series[int, int])
	var verifyCoverLeftHole2 func(t *testing.T, series *sparse.Series[int, int])
	var verifyCoverRightHole1 func(t *testing.T, series *sparse.Series[int, int])
	var verifyCoverRightHole2 func(t *testing.T, series *sparse.Series[int, int])

	var leftHole1Covered = false
	var leftHole2Covered = false
	var rightHole1Covered = false
	var rightHole2Covered = false

	if ok := t.Run("AddEnd", func(t *testing.T) {
		err = series.AddData([]int{40, 50, 60})
		require.NoError(t, err)

		verifyAddEnd = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(40, 60)
			require.NoError(t, err)
			require.Equal(t, []int{40, 50, 60}, res)

			res, err = series.Get(40, 50)
			require.NoError(t, err)
			require.Equal(t, []int{40, 50}, res)

			res, err = series.Get(50, 60)
			require.NoError(t, err)
			require.Equal(t, []int{50, 60}, res)

			res, err = series.Get(45, 55)
			require.NoError(t, err)
			require.Equal(t, []int{50}, res)

			res, err = series.Get(50, 50)
			require.NoError(t, err)
			require.Equal(t, []int{50}, res)

			res, err = series.Get(40, 45)
			require.NoError(t, err)
			require.Equal(t, []int{40}, res)

			res, err = series.Get(55, 60)
			require.NoError(t, err)
			require.Equal(t, []int{60}, res)

			res, err = series.Get(10, 30)
			require.NoError(t, err)
			require.Equal(t, []int{10, 20, 30}, res)

			var hole1Check = require.Error
			if rightHole1Covered {
				hole1Check = require.NoError
			}

			_, err = series.Get(30, 40)
			hole1Check(t, err)

			_, err = series.Get(10, 60)
			hole1Check(t, err)

			_, err = series.Get(35, 40)
			hole1Check(t, err)

			_, err = series.Get(35, 45)
			hole1Check(t, err)

			_, err = series.Get(30, 45)
			hole1Check(t, err)

			_, err = series.Get(30, 50)
			hole1Check(t, err)

			hole2Check := require.Error
			if rightHole2Covered {
				hole2Check = require.NoError
			}

			_, err = series.Get(60, 70)
			hole2Check(t, err)
		}
		verifyAddEnd(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("AddEndOne", func(t *testing.T) {
		err = series.AddData([]int{70})
		require.NoError(t, err)

		verifyAddEndOne = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(70, 70)
			require.NoError(t, err)
			require.Equal(t, []int{70}, res)

			var hole2Check = require.Error
			if rightHole2Covered {
				hole2Check = require.NoError
			}

			_, err = series.Get(60, 70)
			hole2Check(t, err)

			_, err = series.Get(65, 70)
			hole2Check(t, err)

			_, err = series.Get(70, 80)
			require.Error(t, err)

			_, err = series.Get(65, 75)
			require.Error(t, err)

			_, err = series.Get(70, 75)
			require.Error(t, err)

			verifyAddEnd(t, series)
		}
		verifyAddEndOne(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("AddStart", func(t *testing.T) {
		err = series.AddData([]int{-20, -10, 0})
		require.NoError(t, err)

		verifyAddStart = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(-20, 0)
			require.NoError(t, err)
			require.Equal(t, []int{-20, -10, 0}, res)

			res, err = series.Get(-20, -15)
			require.NoError(t, err)
			require.Equal(t, []int{-20}, res)

			res, err = series.Get(-20, -20)
			require.NoError(t, err)
			require.Equal(t, []int{-20}, res)

			res, err = series.Get(-15, -10)
			require.NoError(t, err)
			require.Equal(t, []int{-10}, res)

			res, err = series.Get(-15, -5)
			require.NoError(t, err)
			require.Equal(t, []int{-10}, res)

			res, err = series.Get(-10, -5)
			require.NoError(t, err)
			require.Equal(t, []int{-10}, res)

			res, err = series.Get(-5, 0)
			require.NoError(t, err)
			require.Equal(t, []int{0}, res)

			res, err = series.Get(0, 0)
			require.NoError(t, err)
			require.Equal(t, []int{0}, res)

			leftHole1Check := require.Error
			if leftHole1Covered {
				leftHole1Check = require.NoError
			}

			_, err = series.Get(0, 10)
			leftHole1Check(t, err)

			_, err = series.Get(-20, 10)
			leftHole1Check(t, err)

			_, err = series.Get(-20, 20)
			leftHole1Check(t, err)

			rightHole1Check := require.Error
			if rightHole1Covered {
				rightHole1Check = require.NoError
			}

			_, err = series.Get(-20, 60)
			rightHole1Check(t, err)

			verifyAddEndOne(t, series)
		}
		verifyAddStart(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("AddStartOne", func(t *testing.T) {
		err = series.AddData([]int{-30})
		require.NoError(t, err)

		verifyAddStartOne = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(-30, -30)
			require.NoError(t, err)
			require.Equal(t, []int{-30}, res)

			hole2Check := require.Error
			if leftHole2Covered {
				hole2Check = require.NoError
			}

			_, err = series.Get(-30, -20)
			hole2Check(t, err)

			_, err = series.Get(-30, 10)
			hole2Check(t, err)

			_, err = series.Get(-30, 30)
			hole2Check(t, err)

			allHolesCheck := require.Error
			if leftHole2Covered && rightHole2Covered {
				allHolesCheck = require.NoError
			}

			_, err = series.Get(-30, 70)
			allHolesCheck(t, err)

			verifyAddStart(t, series)
		}
		verifyAddStartOne(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("MergeToInitial", func(t *testing.T) {
		err = series.AddData([]int{8, 9, 10})
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SparseMerge", 0, series.EntriesString())

		res, err := series.Get(8, 30)
		require.NoError(t, err)
		require.Equal(t, []int{8, 9, 10, 20, 30}, res)

		err = series.AddData([]int{7, 8})
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SparseMerge", 0, series.EntriesString())

		res, err = series.Get(7, 30)
		require.NoError(t, err)
		require.Equal(t, []int{7, 8, 9, 10, 20, 30}, res)

		err = series.AddData([]int{30, 31, 32})
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SparseMerge", 0, series.EntriesString())

		res, err = series.Get(7, 32)
		require.NoError(t, err)
		require.Equal(t, []int{7, 8, 9, 10, 20, 30, 31, 32}, res)

		err = series.AddData([]int{32, 33})
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SparseMerge", 0, series.EntriesString())

		verifyMergeToInitial = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err = series.Get(7, 33)
			require.NoError(t, err)
			require.Equal(t, []int{7, 8, 9, 10, 20, 30, 31, 32, 33}, res)

			verifyAddStartOne(t, series)
		}
		verifyMergeToInitial(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("CoverLeftHole1", func(t *testing.T) {
		err = series.AddData([]int{0, 5, 7})
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SparseMerge", 0, series.EntriesString())
		leftHole1Covered = true

		verifyCoverLeftHole1 = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(0, 7)
			require.NoError(t, err)
			require.Equal(t, []int{0, 5, 7}, res)

			res, err = series.Get(-20, 20)
			require.NoError(t, err)
			require.Equal(t, []int{-20, -10, 0, 5, 7, 8, 9, 10, 20}, res)

			verifyMergeToInitial(t, series)
		}
		verifyCoverLeftHole1(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("CoverLeftHole2", func(t *testing.T) {
		err = series.AddData([]int{-30, -20})
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SparseMerge", 0, series.EntriesString())
		leftHole2Covered = true

		verifyCoverLeftHole2 = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(-30, -20)
			require.NoError(t, err)
			require.Equal(t, []int{-30, -20}, res)

			res, err = series.Get(-30, 20)
			require.NoError(t, err)
			require.Equal(t, []int{-30, -20, -10, 0, 5, 7, 8, 9, 10, 20}, res)

			verifyCoverLeftHole1(t, series)
		}
		verifyCoverLeftHole2(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("CoverRightHole1", func(t *testing.T) {
		err = series.AddData([]int{20, 30, 31, 32, 33, 40, 50})
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SparseMerge", 0, series.EntriesString())
		rightHole1Covered = true

		verifyCoverRightHole1 = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(20, 50)
			require.NoError(t, err)
			require.Equal(t, []int{20, 30, 31, 32, 33, 40, 50}, res)

			res, err = series.Get(-30, 50)
			require.NoError(t, err)
			require.Equal(t, []int{-30, -20, -10, 0, 5, 7, 8, 9, 10, 20, 30, 31, 32, 33, 40, 50}, res)

			verifyCoverLeftHole2(t, series)
		}
		verifyCoverRightHole1(t, series)
	}); !ok {
		return
	}

	if ok := t.Run("CoverRightHole2", func(t *testing.T) {
		err = series.AddData([]int{60, 65, 70})
		require.NoError(t, err)
		callerPrintln("TestSparseSeries_SparseMerge", 0, series.EntriesString())
		rightHole2Covered = true

		verifyCoverRightHole2 = func(t *testing.T, series *sparse.Series[int, int]) {
			res, err := series.Get(60, 70)
			require.NoError(t, err)
			require.Equal(t, []int{60, 65, 70}, res)

			res, err = series.Get(-30, 70)
			require.NoError(t, err)
			require.Equal(t, []int{-30, -20, -10, 0, 5, 7, 8, 9, 10, 20, 30, 31, 32, 33, 40, 50, 60, 65, 70}, res)

			verifyCoverRightHole1(t, series)
		}
		verifyCoverRightHole2(t, series)
	}); !ok {
		return
	}
}

func TestSparseSeries_SparseMergeInRangeFirstAndLastDiff(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	err := series.AddData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, res)

	err = series.AddData([]int{40, 50, 60})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(40, 60)
	require.NoError(t, err)
	require.Equal(t, []int{40, 50, 60}, res)

	err = series.AddData([]int{70, 80, 90})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(70, 90)
	require.NoError(t, err)
	require.Equal(t, []int{70, 80, 90}, res)

	err = series.AddData([]int{20, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 90)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 90}, res)
}

func TestSparseSeries_SparseMergeInRangeFirstAndLastSame(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	err := series.AddData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, res)

	err = series.AddData([]int{12, 15, 17, 20})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []int{10, 12, 15, 17, 20, 30}, res)
}

func TestSparseSeries_SparseMergeInRangeFirst(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	err := series.AddData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, res)

	err = series.AddData([]int{40, 50, 60})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(40, 60)
	require.NoError(t, err)
	require.Equal(t, []int{40, 50, 60}, res)

	err = series.AddData([]int{70, 80, 90})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(70, 90)
	require.NoError(t, err)
	require.Equal(t, []int{70, 80, 90}, res)

	err = series.AddData([]int{20, 25, 30, 35, 40, 45, 50, 55, 60, 65})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 65)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65}, res)

	res, err = series.Get(70, 90)
	require.NoError(t, err)
	require.Equal(t, []int{70, 80, 90}, res)

	_, err = series.Get(10, 90)
	require.Error(t, err)
}

func TestSparseSeries_SparseMergeInRangeLast(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	err := series.AddData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, res)

	err = series.AddData([]int{40, 50, 60})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(40, 60)
	require.NoError(t, err)
	require.Equal(t, []int{40, 50, 60}, res)

	err = series.AddData([]int{70, 80, 90})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(70, 90)
	require.NoError(t, err)
	require.Equal(t, []int{70, 80, 90}, res)

	err = series.AddData([]int{35, 40, 45, 50, 55, 60, 65, 70, 75, 80})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(35, 90)
	require.NoError(t, err)
	require.Equal(t, []int{35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 90}, res)

	res, err = series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, res)

	_, err = series.Get(10, 90)
	require.Error(t, err)
}

func TestSparseSeries_SparseMergeInRangeNoHitDiff(t *testing.T) {
	series := intSparseSeries()

	err := series.AddData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, res)

	err = series.AddData([]int{40, 50, 60})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(40, 60)
	require.NoError(t, err)
	require.Equal(t, []int{40, 50, 60}, res)

	err = series.AddData([]int{70, 80, 90})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(70, 90)
	require.NoError(t, err)
	require.Equal(t, []int{70, 80, 90}, res)

	err = series.AddData([]int{35, 40, 45, 50, 55, 60, 65})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(35, 65)
	require.NoError(t, err)
	require.Equal(t, []int{35, 40, 45, 50, 55, 60, 65}, res)

	res, err = series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, res)

	res, err = series.Get(70, 90)
	require.NoError(t, err)
	require.Equal(t, []int{70, 80, 90}, res)

	_, err = series.Get(10, 90)
	require.Error(t, err)
}

func TestSparseSeries_SparseMergeInRangeNoHitSame(t *testing.T) {
	series := intSparseSeries()

	err := series.AddData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, res)

	err = series.AddData([]int{40, 50, 60})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(40, 60)
	require.NoError(t, err)
	require.Equal(t, []int{40, 50, 60}, res)

	err = series.AddData([]int{32, 35, 38})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(32, 38)
	require.NoError(t, err)
	require.Equal(t, []int{32, 35, 38}, res)

	res, err = series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, res)

	res, err = series.Get(40, 60)
	require.NoError(t, err)
	require.Equal(t, []int{40, 50, 60}, res)

	_, err = series.Get(10, 60)
	require.Error(t, err)
}

func TestSparseSeries_GetPeriod(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	err := series.AddData([]int{10, 20, 30})
	require.NoError(t, err)
	err = series.AddData([]int{50, 60, 70})
	require.NoError(t, err)

	p := series.GetPeriod(10, 10)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriod(15, 15)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriod(20, 20)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriod(30, 30)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriod(50, 50)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriod(65, 65)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriod(70, 70)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriod(5, 5)
	require.Nil(t, p)

	p = series.GetPeriod(40, 40)
	require.Nil(t, p)

	p = series.GetPeriod(75, 75)
	require.Nil(t, p)

	p = series.GetPeriod(10, 30)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriod(10, 30)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriod(20, 30)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriod(15, 25)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriod(5, 5)
	require.Nil(t, p)

	p = series.GetPeriod(5, 10)
	require.Nil(t, p)

	p = series.GetPeriod(5, 15)
	require.Nil(t, p)

	p = series.GetPeriod(5, 20)
	require.Nil(t, p)

	p = series.GetPeriod(5, 30)
	require.Nil(t, p)

	p = series.GetPeriod(10, 35)
	require.Nil(t, p)

	p = series.GetPeriod(20, 35)
	require.Nil(t, p)

	p = series.GetPeriod(25, 35)
	require.Nil(t, p)

	p = series.GetPeriod(30, 35)
	require.Nil(t, p)

	p = series.GetPeriod(35, 35)
	require.Nil(t, p)

	p = series.GetPeriod(5, 35)
	require.Nil(t, p)

	p = series.GetPeriod(50, 70)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriod(50, 60)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriod(60, 70)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriod(55, 65)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriod(45, 45)
	require.Nil(t, p)

	p = series.GetPeriod(45, 50)
	require.Nil(t, p)

	p = series.GetPeriod(45, 55)
	require.Nil(t, p)

	p = series.GetPeriod(45, 60)
	require.Nil(t, p)

	p = series.GetPeriod(45, 70)
	require.Nil(t, p)

	p = series.GetPeriod(45, 75)
	require.Nil(t, p)

	p = series.GetPeriod(50, 75)
	require.Nil(t, p)

	p = series.GetPeriod(60, 75)
	require.Nil(t, p)

	p = series.GetPeriod(65, 75)
	require.Nil(t, p)

	p = series.GetPeriod(70, 75)
	require.Nil(t, p)

	p = series.GetPeriod(75, 75)
	require.Nil(t, p)

	p = series.GetPeriod(45, 75)
	require.Nil(t, p)
}

func TestSparseSeries_GetPeriodClosestFromStart(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	err := series.AddData([]int{10, 20, 30})
	require.NoError(t, err)
	err = series.AddPeriod(50, 70, []int{})
	require.NoError(t, err)

	p := series.GetPeriodClosestFromStart(5, false)
	require.Nil(t, p)

	p = series.GetPeriodClosestFromStart(10, false)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromStart(15, false)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromStart(30, false)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromStart(31, false)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromStart(49, false)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromStart(50, false)
	require.NotNil(t, p)
	require.Empty(t, must2(p.GetAll()))

	p = series.GetPeriodClosestFromStart(65, false)
	require.NotNil(t, p)
	require.Empty(t, must2(p.GetAll()))

	p = series.GetPeriodClosestFromStart(70, false)
	require.NotNil(t, p)
	require.Empty(t, must2(p.GetAll()))

	p = series.GetPeriodClosestFromStart(71, false)
	require.NotNil(t, p)
	require.Empty(t, must2(p.GetAll()))
}

func TestSparseSeries_GetPeriodClosestFromEnd(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	err := series.AddPeriod(10, 30, []int{})
	require.NoError(t, err)
	err = series.AddData([]int{50, 60, 70})
	require.NoError(t, err)

	p := series.GetPeriodClosestFromEnd(5, false)
	require.NotNil(t, p)
	require.Empty(t, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(10, false)
	require.NotNil(t, p)
	require.Empty(t, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(15, false)
	require.NotNil(t, p)
	require.Empty(t, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(30, false)
	require.NotNil(t, p)
	require.Empty(t, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(31, false)
	require.NotNil(t, p)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(49, false)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(50, false)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(65, false)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(70, false)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(71, false)
	require.Nil(t, p)
}

func TestSparseSeries_GetNonEmptyPeriodClosestFromStart(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	err := series.AddPeriod(0, 4, []int{})
	require.NoError(t, err)
	err = series.AddData([]int{10, 20, 30})
	require.NoError(t, err)
	err = series.AddPeriod(50, 70, []int{})
	require.NoError(t, err)

	p := series.GetPeriodClosestFromStart(-1, true)
	require.Nil(t, p)

	p = series.GetPeriodClosestFromStart(4, true)
	require.Nil(t, p)

	p = series.GetPeriodClosestFromStart(5, true)
	require.Nil(t, p)

	p = series.GetPeriodClosestFromStart(10, true)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromStart(15, true)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromStart(30, true)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromStart(31, true)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromStart(49, true)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromStart(50, true)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromStart(65, true)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromStart(70, true)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromStart(71, true)
	require.Equal(t, []int{10, 20, 30}, must2(p.GetAll()))
}

func TestSparseSeries_GetNonEmptyPeriodClosestFromEnd(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	err := series.AddPeriod(10, 30, []int{})
	require.NoError(t, err)
	err = series.AddData([]int{50, 60, 70})
	require.NoError(t, err)
	err = series.AddPeriod(80, 100, []int{})
	require.NoError(t, err)

	p := series.GetPeriodClosestFromEnd(5, true)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(10, true)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(15, true)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(30, true)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(31, true)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(49, true)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(50, true)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(65, true)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(70, true)
	require.Equal(t, []int{50, 60, 70}, must2(p.GetAll()))

	p = series.GetPeriodClosestFromEnd(71, true)
	require.Nil(t, p)

	p = series.GetPeriodClosestFromEnd(80, true)
	require.Nil(t, p)

	p = series.GetPeriodClosestFromEnd(101, true)
	require.Nil(t, p)
}

func TestSparseSeries_Continuous_Ends(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	err := series.AddData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, res)

	err = series.AddData([]int{7, 8, 9})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(7, 30)
	require.NoError(t, err)
	require.Equal(t, []int{7, 8, 9, 10, 20, 30}, res)

	res, err = series.Get(10, 10)
	require.NoError(t, err)
	require.Equal(t, []int{10}, res)

	res, err = series.Get(10, 11)
	require.NoError(t, err)
	require.Equal(t, []int{10}, res)

	res, err = series.Get(11, 11)
	require.NoError(t, err)
	require.Empty(t, res)

	err = series.AddData([]int{31, 32, 33})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(7, 33)
	require.NoError(t, err)
	require.Equal(t, []int{7, 8, 9, 10, 20, 30, 31, 32, 33}, res)

	err = series.AddData([]int{6})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(6, 33)
	require.NoError(t, err)
	require.Equal(t, []int{6, 7, 8, 9, 10, 20, 30, 31, 32, 33}, res)

	err = series.AddData([]int{34})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(6, 34)
	require.NoError(t, err)
	require.Equal(t, []int{6, 7, 8, 9, 10, 20, 30, 31, 32, 33, 34}, res)

	err = series.AddPeriod(3, 5, []int{4})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(3, 34)
	require.NoError(t, err)
	require.Equal(t, []int{4, 6, 7, 8, 9, 10, 20, 30, 31, 32, 33, 34}, res)

	res, err = series.Get(2, 3)
	require.Error(t, err, res)

	res, err = series.Get(3, 3)
	require.NoError(t, err)
	require.Empty(t, res)

	res, err = series.Get(3, 4)
	require.NoError(t, err)
	require.Equal(t, []int{4}, res)

	res, err = series.Get(4, 4)
	require.NoError(t, err)
	require.Equal(t, []int{4}, res)

	err = series.AddPeriod(35, 37, []int{36})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(3, 37)
	require.NoError(t, err)
	require.Equal(t, []int{4, 6, 7, 8, 9, 10, 20, 30, 31, 32, 33, 34, 36}, res)

	res, err = series.Get(37, 38)
	require.Error(t, err, res)

	res, err = series.Get(37, 37)
	require.NoError(t, err)
	require.Empty(t, res)

	res, err = series.Get(36, 37)
	require.NoError(t, err)
	require.Equal(t, []int{36}, res)

	res, err = series.Get(36, 36)
	require.NoError(t, err)
	require.Equal(t, []int{36}, res)
}

func TestSparseSeries_Continuous_MiddleStart(t *testing.T) {
	t.Parallel()

	series := intSparseSeries()

	err := series.AddData([]int{10, 20, 30})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30}, res)

	err = series.AddData([]int{40, 50, 60})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(40, 60)
	require.NoError(t, err)
	require.Equal(t, []int{40, 50, 60}, res)

	_, err = series.Get(10, 33)
	require.Error(t, err)

	err = series.AddData([]int{31, 32, 33})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 33)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30, 31, 32, 33}, res)

	_, err = series.Get(10, 39)
	require.Error(t, err)
	_, err = series.Get(10, 40)
	require.Error(t, err)
	_, err = series.Get(33, 34)
	require.Error(t, err)
	_, err = series.Get(33, 40)
	require.Error(t, err)
	_, err = series.Get(33, 39)
	require.Error(t, err)
	_, err = series.Get(34, 40)
	require.Error(t, err)

	err = series.AddData([]int{37, 38, 39})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(37, 60)
	require.NoError(t, err)
	require.Equal(t, []int{37, 38, 39, 40, 50, 60}, res)

	_, err = series.Get(33, 36)
	require.Error(t, err)
	_, err = series.Get(34, 37)
	require.Error(t, err)
	_, err = series.Get(36, 37)
	require.Error(t, err)
	_, err = series.Get(36, 36)
	require.Error(t, err)
	_, err = series.Get(36, 60)
	require.Error(t, err)
	_, err = series.Get(33, 60)
	require.Error(t, err)

	_, err = series.Get(34, 36)
	require.Error(t, err)
	_, err = series.Get(33, 37)
	require.Error(t, err)

	err = series.AddData([]int{34, 35, 36})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(34, 36)
	require.NoError(t, err)
	require.Equal(t, []int{34, 35, 36}, res)
	res, err = series.Get(33, 37)
	require.NoError(t, err)
	require.Equal(t, []int{33, 34, 35, 36, 37}, res)
	res, err = series.Get(10, 60)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 50, 60}, res)

	_, err = series.Get(9, 60)
	require.Error(t, err)
	_, err = series.Get(10, 61)
	require.Error(t, err)
	_, err = series.Get(9, 61)
	require.Error(t, err)
}

func TestSparseSeries_OverwriteStart(t *testing.T) {
	t.Parallel()

	type Elem struct {
		Key int
		Val string
	}
	series := sparse.NewSeries[Elem, int](
		sparse.NewArrayData,
		func(data *Elem) int { return data.Key },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		func(smaller, bigger int) bool { return bigger-smaller == 1 },
	)

	err := series.AddData([]Elem{{10, "a"}, {20, "b"}, {30, "c"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []Elem{{10, "a"}, {20, "b"}, {30, "c"}}, res)

	err = series.AddData([]Elem{{5, "z1"}, {10, "z2"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(5, 30)
	require.NoError(t, err)
	require.Equal(t, []Elem{{5, "z1"}, {10, "z2"}, {20, "b"}, {30, "c"}}, res)
}

func TestSparseSeries_OverwriteEnd(t *testing.T) {
	t.Parallel()

	type Elem struct {
		Key int
		Val string
	}
	series := sparse.NewSeries[Elem, int](
		sparse.NewArrayData,
		func(data *Elem) int { return data.Key },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		func(smaller, bigger int) bool { return bigger-smaller == 1 },
	)

	err := series.AddData([]Elem{{10, "a"}, {20, "b"}, {30, "c"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []Elem{{10, "a"}, {20, "b"}, {30, "c"}}, res)

	err = series.AddData([]Elem{{30, "d"}, {40, "e"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 40)
	require.NoError(t, err)
	require.Equal(t, []Elem{{10, "a"}, {20, "b"}, {30, "d"}, {40, "e"}}, res)
}

func TestSparseSeries_OverwriteHoles(t *testing.T) {
	t.Parallel()

	type Elem struct {
		Key int
		Val string
	}
	series := sparse.NewSeries[Elem, int](
		sparse.NewArrayData,
		func(data *Elem) int { return data.Key },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := series.AddData([]Elem{{50, "d"}, {60, "e"}, {70, "f"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(50, 70)
	require.NoError(t, err)
	require.Equal(t, []Elem{{50, "d"}, {60, "e"}, {70, "f"}}, res)

	err = series.AddData([]Elem{{10, "a"}, {20, "b"}, {30, "c"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []Elem{{10, "a"}, {20, "b"}, {30, "c"}}, res)

	err = series.AddData([]Elem{{90, "g"}, {100, "h"}, {110, "i"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(90, 110)
	require.NoError(t, err)
	require.Equal(t, []Elem{{90, "g"}, {100, "h"}, {110, "i"}}, res)

	_, err = series.Get(10, 110)
	require.Error(t, err)

	err = series.AddData([]Elem{{25, "z1"}, {30, "z2"}, {40, "z3"}, {50, "z4"}, {55, "z5"}, {60, "z6"}, {70, "z7"}, {80, "z8"}, {90, "z9"}, {95, "z10"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 110)
	require.NoError(t, err)
	require.Equal(t, []Elem{{10, "a"}, {20, "b"}, {25, "z1"}, {30, "z2"}, {40, "z3"}, {50, "z4"}, {55, "z5"}, {60, "z6"}, {70, "z7"}, {80, "z8"}, {90, "z9"}, {95, "z10"}, {100, "h"}, {110, "i"}}, res)
}

func TestSparseSeries_OverwriteIsle(t *testing.T) {
	t.Parallel()

	type Elem struct {
		Key int
		Val string
	}
	series := sparse.NewSeries[Elem, int](
		sparse.NewArrayData,
		func(data *Elem) int { return data.Key },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := series.AddData([]Elem{{50, "d"}, {60, "e"}, {70, "f"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(50, 70)
	require.NoError(t, err)
	require.Equal(t, []Elem{{50, "d"}, {60, "e"}, {70, "f"}}, res)

	err = series.AddData([]Elem{{10, "a"}, {20, "b"}, {30, "c"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []Elem{{10, "a"}, {20, "b"}, {30, "c"}}, res)

	err = series.AddData([]Elem{{90, "g"}, {100, "h"}, {110, "i"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(90, 110)
	require.NoError(t, err)
	require.Equal(t, []Elem{{90, "g"}, {100, "h"}, {110, "i"}}, res)

	_, err = series.Get(10, 110)
	require.Error(t, err)

	err = series.AddData([]Elem{{40, "z3"}, {50, "z4"}, {55, "z5"}, {60, "z6"}, {70, "z7"}, {80, "z8"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(40, 80)
	require.NoError(t, err)
	require.Equal(t, []Elem{{40, "z3"}, {50, "z4"}, {55, "z5"}, {60, "z6"}, {70, "z7"}, {80, "z8"}}, res)

	err = series.AddData([]Elem{{10, "a"}, {20, "b"}, {30, "c"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []Elem{{10, "a"}, {20, "b"}, {30, "c"}}, res)

	res, err = series.Get(90, 110)
	require.NoError(t, err)
	require.Equal(t, []Elem{{90, "g"}, {100, "h"}, {110, "i"}}, res)

	_, err = series.Get(10, 110)
	require.Error(t, err)
}

func TestSparseSeries_OverwriteMiddle(t *testing.T) {
	t.Parallel()

	type Elem struct {
		Key int
		Val string
	}
	series := sparse.NewSeries[Elem, int](
		sparse.NewArrayData,
		func(data *Elem) int { return data.Key },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := series.AddData([]Elem{{50, "d"}, {60, "e"}, {70, "f"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(50, 70)
	require.NoError(t, err)
	require.Equal(t, []Elem{{50, "d"}, {60, "e"}, {70, "f"}}, res)

	err = series.AddData([]Elem{{10, "a"}, {20, "b"}, {30, "c"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []Elem{{10, "a"}, {20, "b"}, {30, "c"}}, res)

	err = series.AddData([]Elem{{90, "g"}, {100, "h"}, {110, "i"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(90, 110)
	require.NoError(t, err)
	require.Equal(t, []Elem{{90, "g"}, {100, "h"}, {110, "i"}}, res)

	_, err = series.Get(10, 110)
	require.Error(t, err)

	err = series.AddData([]Elem{{55, "z3"}, {60, "z4"}, {65, "z5"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(50, 70)
	require.NoError(t, err)
	require.Equal(t, []Elem{{50, "d"}, {55, "z3"}, {60, "z4"}, {65, "z5"}, {70, "f"}}, res)

	err = series.AddData([]Elem{{10, "a"}, {20, "b"}, {30, "c"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []Elem{{10, "a"}, {20, "b"}, {30, "c"}}, res)

	res, err = series.Get(90, 110)
	require.NoError(t, err)
	require.Equal(t, []Elem{{90, "g"}, {100, "h"}, {110, "i"}}, res)

	_, err = series.Get(10, 110)
	require.Error(t, err)
}

func TestSparseSeries_OverwriteInRangeStart(t *testing.T) {
	t.Parallel()

	type Elem struct {
		Key int
		Val string
	}
	series := sparse.NewSeries[Elem, int](
		sparse.NewArrayData,
		func(data *Elem) int { return data.Key },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := series.AddData([]Elem{{50, "d"}, {60, "e"}, {70, "f"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(50, 70)
	require.NoError(t, err)
	require.Equal(t, []Elem{{50, "d"}, {60, "e"}, {70, "f"}}, res)

	err = series.AddData([]Elem{{10, "a"}, {20, "b"}, {30, "c"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []Elem{{10, "a"}, {20, "b"}, {30, "c"}}, res)

	_, err = series.Get(10, 70)
	require.Error(t, err)

	err = series.AddData([]Elem{{40, "z3"}, {50, "z4"}, {55, "z5"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(40, 70)
	require.NoError(t, err)
	require.Equal(t, []Elem{{40, "z3"}, {50, "z4"}, {55, "z5"}, {60, "e"}, {70, "f"}}, res)

	res, err = series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []Elem{{10, "a"}, {20, "b"}, {30, "c"}}, res)

	_, err = series.Get(10, 70)
	require.Error(t, err)
}

func TestSparseSeries_OverwriteInRangeEnd(t *testing.T) {
	t.Parallel()

	type Elem struct {
		Key int
		Val string
	}
	series := sparse.NewSeries[Elem, int](
		sparse.NewArrayData,
		func(data *Elem) int { return data.Key },
		func(idx1, idx2 int) int { return idx1 - idx2 },
		nil,
	)

	err := series.AddData([]Elem{{50, "d"}, {60, "e"}, {70, "f"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err := series.Get(50, 70)
	require.NoError(t, err)
	require.Equal(t, []Elem{{50, "d"}, {60, "e"}, {70, "f"}}, res)

	err = series.AddData([]Elem{{10, "a"}, {20, "b"}, {30, "c"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 30)
	require.NoError(t, err)
	require.Equal(t, []Elem{{10, "a"}, {20, "b"}, {30, "c"}}, res)

	_, err = series.Get(10, 70)
	require.Error(t, err)

	err = series.AddData([]Elem{{25, "z1"}, {30, "z2"}, {40, "z3"}})
	require.NoError(t, err)
	println(0, series.EntriesString())
	res, err = series.Get(10, 40)
	require.NoError(t, err)
	require.Equal(t, []Elem{{10, "a"}, {20, "b"}, {25, "z1"}, {30, "z2"}, {40, "z3"}}, res)

	res, err = series.Get(50, 70)
	require.NoError(t, err)
	require.Equal(t, []Elem{{50, "d"}, {60, "e"}, {70, "f"}}, res)

	_, err = series.Get(10, 70)
	require.Error(t, err)
}
