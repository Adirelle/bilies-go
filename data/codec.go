// +build generate

package data

//go:generate codecgen -o codec.generated.go esResponse.go inputRecord.go record.go
