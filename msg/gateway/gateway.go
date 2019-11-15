//go:generate msgp -io=false -tests=false

package gateway

import (
	"github.com/baudb/baudb/msg"
)

type SelectRequest struct {
	Query   string `msg:"query"`
	Start   string `msg:"start"`
	End     string `msg:"end"`
	Offset  int    `msg:"offset"`
	Limit   int    `msg:"limit"`
	Timeout string `msg:"timeout"`
}

type SelectResponse struct {
	Result   string         `msg:"result"`
	Status   msg.StatusCode `msg:"status"`
	ErrorMsg string         `msg:"errorMsg"`
}

type SeriesLabelsRequest struct {
	Matches []string `msg:"matches"`
	Start   string   `msg:"start"`
	End     string   `msg:"end"`
	Timeout string   `msg:"timeout"`
}

type SeriesLabelsResponse struct {
	Labels   [][]msg.Label  `msg:"labels"`
	Status   msg.StatusCode `msg:"status"`
	ErrorMsg string         `msg:"errorMsg"`
}

type LabelValuesRequest struct {
	Name       string `msg:"name"`
	Constraint string `msg:"constraint"`
	Timeout    string `msg:"timeout"`
}

//msgp:tuple AddRequest
type AddRequest struct {
	Series []*msg.Series `msg:"series"`
}
