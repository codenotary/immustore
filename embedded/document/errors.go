/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package document

import (
	"errors"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
)

var (
	ErrIllegalArguments        = store.ErrIllegalArguments
	ErrUnsupportedType         = errors.New("unsupported type")
	ErrUnexpectedValue         = errors.New("unexpected value")
	ErrCollectionAlreadyExists = errors.New("collection already exists")
	ErrCollectionDoesNotExist  = errors.New("collection does not exist")
	ErrMaxLengthExceeded       = errors.New("max length exceeded")
	ErrMultipleDocumentsFound  = errors.New("multiple documents found")
	ErrDocumentNotFound        = errors.New("document not found")
	ErrNoMoreDocuments         = errors.New("no more documents")
	ErrFieldAlreadyExists      = errors.New("field already exists")
	ErrFieldDoesNotExist       = errors.New("field does not exist")
	ErrReservedName            = errors.New("reserved name")
	ErrLimitedIndexCreation    = errors.New("unique index creation is only supported on empty collections")
	ErrConflict                = errors.New("conflict due to uniqueness contraint violation or read document was updated by another transaction")
	ErrInvalidHex              = errors.New("value is not valid hex, has to have even number of characters [0-9a-f]")
)

func mayTranslateError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, sql.ErrTableAlreadyExists) {
		return ErrCollectionAlreadyExists
	}

	if errors.Is(err, sql.ErrTableDoesNotExist) {
		return ErrCollectionDoesNotExist
	}

	if errors.Is(err, sql.ErrNoMoreRows) {
		return ErrNoMoreDocuments
	}

	if errors.Is(err, sql.ErrColumnAlreadyExists) {
		return ErrFieldAlreadyExists
	}

	if errors.Is(err, sql.ErrColumnDoesNotExist) {
		return ErrFieldDoesNotExist
	}

	if errors.Is(err, sql.ErrLimitedIndexCreation) {
		return ErrLimitedIndexCreation
	}

	if errors.Is(err, store.ErrTxReadConflict) {
		return ErrConflict
	}

	if errors.Is(err, store.ErrKeyAlreadyExists) {
		return ErrConflict
	}

	return err
}
