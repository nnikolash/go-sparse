package sparse

import "fmt"

type MissingPeriodError[Index any] struct {
	PeriodStart Index
	PeriodEnd   Index
}

func (e *MissingPeriodError[Index]) Error() string {
	return fmt.Sprintf("series missing period: %v - %v", e.PeriodStart, e.PeriodEnd)
}
