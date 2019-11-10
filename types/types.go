package types

type SetItemInput struct {
	Key   string
	Value interface{}
}

type BatchSetItemInput struct {
	Keys   []string
	Values interface{}
}

type GetItemInput struct {
	Key   string
	Value interface{}
}

type DeleteItemInput struct {
	Key string
}
