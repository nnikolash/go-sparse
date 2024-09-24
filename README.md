# go-sparse - Sparse series storage for Go

## What this library can do?

* Provides easy access for **sorted non-continuous series** of any data.
* Can retrieve data for specific period, or answer if requested period is fully present.
* Supports adding and updating periods and data entries to series.
* Does NOT support deletion.
* Can account of continuity of descrete keys (e.g. integers [1; 3] are followed by [4; 5] without gap between tham, which is not true for floats).
* Allows to implement any type of underlaying storage.

This library was created as base container for [go-timeline](https://github.com/nnikolash/go-timeline). 

## Usage

###### Have some data with index field

```
type TestEvent struct {
   Time time.Time // In this case Time is index, but it could be anything
   Data int
}
```

###### Create sparse series

```
// Data storate factory
dataStorageFactory := sparse.NewArrayData[TestEvent, time.Time]

// Getter of index
getIdx := func(data *TestEvent) time.Time {
   return data.Time
}

// Comparator of index
cmp := func(idx1, idx2 time.Time) int {
   return idx1.Compare(idx2)
}

// Optional comparator to specify if index are continuous (so nothing can fit inbetween them)
areContinuous := func(smaller, bigger time.Time) bool {
   return smaller.UnixNano() == bigger.UnixNano()+1
}

series := sparse.NewSeries(dataStorageFactory, getIdx, cmp, areContinuous)
```

###### Add some data

```
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
```

###### Retrieve data from container

```
// Getting everything in range [1; 3), or [1; 2.000000005] if to be exact
res, err := series.Get(time.Unix(1, 0), time.Unix(2, 5))
require.NoError(t, err)
require.Equal(t, []TestEvent{
  {Time: time.Unix(1, 0), Data: 1},
  {Time: time.Unix(2, 0), Data: 2},
}, res)
```

## Custom data storage

Indexed data may be stored in any type of storage - `ArrayData` is just an example of basic storage in memory.
To support your own storage create implementation for `SeriesData` inteface and `SeriesDataFactory` function.

## Examples

See folder `examples` or files `*_test.go` for more examples.
