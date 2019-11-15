package filter

import backendmsg "github.com/baudb/baudb/msg/backend"

type Filter func(v interface{}) bool

func MergeFilter(filters []*backendmsg.Filter) (Filter, error) {
	fns := make([]func(v interface{}) bool, 0, len(filters))

	for _, filter := range filters {
		f, err := filter.FilterFn()
		if err != nil {
			return nil, err
		}
		fns = append(fns, f)
	}

	return func(v interface{}) bool {
		if len(fns) > 0 {
			for _, f := range fns {
				if !f(v) {
					return false
				}
			}
		}
		return true
	}, nil
}
