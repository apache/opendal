// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package opendal

import (
	"context"
	"errors"
	"fmt"
)

// ErrorCode is all kinds of ErrorCode of opendal
type ErrorCode int32

const (
	// OpenDAL don't know what happened here, and no actions other than just
	// returning it back. For example, s3 returns an internal service error.
	CodeUnexpected ErrorCode = iota
	// Underlying service doesn't support this operation.
	CodeUnsupported
	// The config for backend is invalid.
	CodeConfigInvalid
	// The given path is not found.
	CodeNotFound
	// The given path doesn't have enough permission for this operation
	CodePermissioDenied
	// The given path is a directory.
	CodeIsADirectory
	// The given path is not a directory.
	CodeNotADirectory
	// The given path already exists thus we failed to the specified operation on it.
	CodeAlreadyExists
	// Requests that sent to this path is over the limit, please slow down.
	CodeRateLimited
	// The given file paths are same.
	CodeIsSameFile
	// A condition supplied through the OpenDAL operation evaluated to false.
	CodeConditionNotMatch
	// The range of the content is not satisfied.
	//
	// OpenDAL returns this error to indicate that the range of the read request is not satisfied.
	CodeRangeNotSatisfied
	// The operation conflicts with the current or transitional state of the resource.
	//
	// This error code does not indicate whether retrying the same operation is safe.
	CodeConflict
	// The operation was cancelled via a context.
	CodeCancelled
)

func parseError(ctx context.Context, err *opendalError) error {
	if err == nil {
		return nil
	}
	defer ffiErrorFree.symbol(ctx)(err)
	return &Error{
		code:    ErrorCode(err.code),
		message: string(parseBytes(err.message)),
	}
}

type Error struct {
	code    ErrorCode
	message string
	cause   error
}

func (e *Error) Error() string {
	return fmt.Sprintf("%d %s", e.code, e.message)
}

func (e *Error) Code() ErrorCode {
	return e.code
}

func (e *Error) Message() string {
	return e.message
}

// Unwrap returns the underlying cause of the error, if any.
//
// For cancelled operations (CodeCancelled), Unwrap returns context.Canceled or
// context.DeadlineExceeded so callers can match them with errors.Is.
func (e *Error) Unwrap() error {
	return e.cause
}

func newCanceledError(ctxErr error) error {
	return &Error{
		code:    CodeCancelled,
		message: ctxErr.Error(),
		cause:   ctxErr,
	}
}

func wrapCanceledError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	var opErr *Error
	if !errors.As(err, &opErr) || opErr.code != CodeCancelled {
		return err
	}
	cause := context.Canceled
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			cause = ctxErr
		}
	}
	return &Error{
		code:    CodeCancelled,
		message: opErr.message,
		cause:   cause,
	}
}

var ffiErrorFree = newDirectFFI[func(e *opendalError)]("opendal_error_free")
