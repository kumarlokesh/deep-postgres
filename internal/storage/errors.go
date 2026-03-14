package storage

import "fmt"

// StorageError is the sentinel error type for all storage-layer failures.
// Callers can match specific variants with errors.Is or a type switch.
type StorageError struct {
	Code    StorageErrorCode
	Message string
}

func (e *StorageError) Error() string {
	return fmt.Sprintf("storage[%s]: %s", e.Code, e.Message)
}

// StorageErrorCode classifies the failure.
type StorageErrorCode string

const (
	ErrPageFull          StorageErrorCode = "page_full"
	ErrInvalidItemIndex  StorageErrorCode = "invalid_item_index"
	ErrInvalidPageSize   StorageErrorCode = "invalid_page_size"
	ErrInvalidPageLayout StorageErrorCode = "invalid_page_layout"
	ErrTupleTooLarge     StorageErrorCode = "tuple_too_large"
	ErrBufferExhausted   StorageErrorCode = "buffer_exhausted"
	ErrInvalidBufferId   StorageErrorCode = "invalid_buffer_id"
	ErrBufferNotPinned   StorageErrorCode = "buffer_not_pinned"
)

func errPageFull(required, available int) error {
	return &StorageError{
		Code:    ErrPageFull,
		Message: fmt.Sprintf("required %d bytes, available %d bytes", required, available),
	}
}

func errInvalidItemIndex(index int) error {
	return &StorageError{Code: ErrInvalidItemIndex, Message: fmt.Sprintf("index %d out of range", index)}
}

func errInvalidPageSize(expected, actual int) error {
	return &StorageError{
		Code:    ErrInvalidPageSize,
		Message: fmt.Sprintf("expected %d, got %d", expected, actual),
	}
}

func errInvalidPageLayout(msg string) error {
	return &StorageError{Code: ErrInvalidPageLayout, Message: msg}
}

func errBufferExhausted() error {
	return &StorageError{Code: ErrBufferExhausted, Message: "all buffers are pinned"}
}

func errInvalidBufferId(id BufferId) error {
	return &StorageError{Code: ErrInvalidBufferId, Message: fmt.Sprintf("id %d", id)}
}

func errBufferNotPinned(id BufferId) error {
	return &StorageError{Code: ErrBufferNotPinned, Message: fmt.Sprintf("buffer %d is not pinned", id)}
}
