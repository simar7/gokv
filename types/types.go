package types

type SetItemInput struct {
	BucketName string
	Key        string
	Value      interface{}
}

type BatchSetItemInput struct {
	BucketName string
	Keys       []string
	Values     interface{}
}

type GetItemInput struct {
	BucketName string
	Key        string
	Value      interface{}
}

type DeleteItemInput struct {
	BucketName string
	Key        string
}
