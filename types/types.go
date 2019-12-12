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

type ScanInput struct {
	BucketName string
}

type ScanOutput struct {
	Keys   []string `dynamodbav:"k"`
	Values [][]byte `dynamodbav:"v"`
}

type StoreInfo struct {
	Name string
	Size int64
}
