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
	"context"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/protomodel"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

var docPrefix = []byte{3}

func makeEngine(t *testing.T) *Engine {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)

	t.Cleanup(func() {
		err := st.Close()
		if !t.Failed() {
			// Do not pollute error output if test has already failed
			require.NoError(t, err)
		}
	})

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(docPrefix))
	require.NoError(t, err)

	err = engine.CopyCatalogToTx(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	return engine
}

func TestEngineWithInvalidOptions(t *testing.T) {
	_, err := NewEngine(nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = NewEngine(nil, DefaultOptions())
	require.ErrorIs(t, err, ErrIllegalArguments)
}

func TestCreateCollection(t *testing.T) {
	engine := makeEngine(t)

	t.Run("collection creation should fail with invalid collection name", func(t *testing.T) {
		err := engine.CreateCollection(
			context.Background(),
			"admin",
			"1invalidCollectionName",
			"",
			[]*protomodel.Field{
				{Name: "number", Type: protomodel.FieldType_DOUBLE},
				{Name: "name", Type: protomodel.FieldType_STRING},
				{Name: "pin", Type: protomodel.FieldType_INTEGER},
				{Name: "country", Type: protomodel.FieldType_STRING},
				{Name: "active", Type: protomodel.FieldType_BOOLEAN},
			},
			[]*protomodel.Index{
				{Fields: []string{"number"}},
				{Fields: []string{"name"}},
				{Fields: []string{"pin"}},
				{Fields: []string{"country"}},
				{Fields: []string{"address.street"}},
				{Fields: []string{"active"}},
			},
		)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("collection creation should fail with invalid collection name", func(t *testing.T) {
		err := engine.CreateCollection(
			context.Background(),
			"admin",
			"collection",
			"",
			[]*protomodel.Field{
				{Name: "number", Type: protomodel.FieldType_DOUBLE},
				{Name: "name", Type: protomodel.FieldType_STRING},
				{Name: "pin", Type: protomodel.FieldType_INTEGER},
				{Name: "country", Type: protomodel.FieldType_STRING},
			},
			[]*protomodel.Index{
				{Fields: []string{"number"}},
				{Fields: []string{"name"}},
				{Fields: []string{"pin"}},
				{Fields: []string{"country"}},
				{Fields: []string{"address.street"}},
			},
		)
		require.ErrorIs(t, err, ErrReservedName)
	})

	collectionName := "my-collection"

	t.Run("collection creation should fail with invalid field name", func(t *testing.T) {
		err := engine.CreateCollection(
			context.Background(),
			"admin",
			collectionName,
			"",
			[]*protomodel.Field{
				{Name: DocumentBLOBField, Type: protomodel.FieldType_DOUBLE},
			},
			[]*protomodel.Index{
				{Fields: []string{DocumentBLOBField}},
			},
		)
		require.ErrorIs(t, err, ErrReservedName)
	})

	t.Run("collection creation should fail with reserved field name", func(t *testing.T) {
		err := engine.CreateCollection(
			context.Background(),
			"admin",
			collectionName,
			"",
			[]*protomodel.Field{
				{Name: "document", Type: protomodel.FieldType_DOUBLE},
			},
			[]*protomodel.Index{
				{Fields: []string{"document"}},
			},
		)
		require.ErrorIs(t, err, ErrReservedName)
	})

	t.Run("collection creation should fail with invalid field name", func(t *testing.T) {
		err := engine.CreateCollection(
			context.Background(),
			"admin",
			collectionName,
			"",
			[]*protomodel.Field{
				{Name: "_id", Type: protomodel.FieldType_DOUBLE},
			},
			nil,
		)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("collection creation should fail with invalid document id field name", func(t *testing.T) {
		err := engine.CreateCollection(
			context.Background(),
			"admin",
			collectionName,
			"invalid.docid",
			[]*protomodel.Field{
				{Name: "number", Type: protomodel.FieldType_DOUBLE},
				{Name: "name", Type: protomodel.FieldType_STRING},
				{Name: "pin", Type: protomodel.FieldType_INTEGER},
				{Name: "country", Type: protomodel.FieldType_STRING},
			},
			[]*protomodel.Index{
				{Fields: []string{"number"}},
				{Fields: []string{"name"}},
				{Fields: []string{"pin"}},
				{Fields: []string{"country"}},
				{Fields: []string{"address.street"}},
			},
		)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("collection creation should fail with invalid document id field name", func(t *testing.T) {
		err := engine.CreateCollection(
			context.Background(),
			"admin",
			collectionName,
			DocumentBLOBField,
			[]*protomodel.Field{
				{Name: "number", Type: protomodel.FieldType_DOUBLE},
				{Name: "name", Type: protomodel.FieldType_STRING},
				{Name: "pin", Type: protomodel.FieldType_INTEGER},
				{Name: "country", Type: protomodel.FieldType_STRING},
			},
			[]*protomodel.Index{
				{Fields: []string{"number"}},
				{Fields: []string{"name"}},
				{Fields: []string{"pin"}},
				{Fields: []string{"country"}},
				{Fields: []string{"address.street"}},
			},
		)
		require.ErrorIs(t, err, ErrReservedName)
	})

	t.Run("collection creation should fail with invalid field name", func(t *testing.T) {
		err := engine.CreateCollection(
			context.Background(),
			"admin",
			collectionName,
			"",
			[]*protomodel.Field{
				{Name: "1number", Type: protomodel.FieldType_DOUBLE},
				{Name: "name", Type: protomodel.FieldType_STRING},
				{Name: "pin", Type: protomodel.FieldType_INTEGER},
				{Name: "country", Type: protomodel.FieldType_STRING},
			},
			[]*protomodel.Index{
				{Fields: []string{"1number"}},
				{Fields: []string{"name"}},
				{Fields: []string{"pin"}},
				{Fields: []string{"country"}},
			},
		)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("collection creation should fail with unexistent field", func(t *testing.T) {
		err := engine.CreateCollection(
			context.Background(),
			"admin",
			collectionName,
			"",
			[]*protomodel.Field{
				{Name: "number", Type: protomodel.FieldType_DOUBLE},
				{Name: "name", Type: protomodel.FieldType_STRING},
				{Name: "pin", Type: protomodel.FieldType_INTEGER},
				{Name: "country", Type: protomodel.FieldType_STRING},
			},
			[]*protomodel.Index{
				{Fields: []string{"number"}},
				{Fields: []string{"name"}},
				{Fields: []string{"pin"}},
				{Fields: []string{"country"}},
				{Fields: []string{"address.street"}},
			},
		)
		require.ErrorIs(t, err, ErrFieldDoesNotExist)
	})

	t.Run("collection creation should fail with invalid index", func(t *testing.T) {
		err := engine.CreateCollection(
			context.Background(),
			"admin",
			collectionName,
			"",
			[]*protomodel.Field{
				{Name: "number", Type: protomodel.FieldType_DOUBLE},
			},
			[]*protomodel.Index{
				{Fields: []string{}},
			},
		)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("collection creation should fail with invalid index", func(t *testing.T) {
		err := engine.CreateCollection(
			context.Background(),
			"admin",
			collectionName,
			"",
			[]*protomodel.Field{},
			[]*protomodel.Index{
				{Fields: []string{"_id"}},
			},
		)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("collection creation should fail with invalid index", func(t *testing.T) {
		err := engine.CreateCollection(
			context.Background(),
			"admin",
			collectionName,
			"",
			[]*protomodel.Field{},
			[]*protomodel.Index{
				{Fields: []string{"_id", "collection"}},
			},
		)
		require.ErrorIs(t, err, ErrReservedName)
	})

	err := engine.CreateCollection(
		context.Background(),
		"admin",
		collectionName,
		"doc-id",
		[]*protomodel.Field{
			{Name: "number", Type: protomodel.FieldType_DOUBLE},
			{Name: "name", Type: protomodel.FieldType_STRING},
			{Name: "pin", Type: protomodel.FieldType_INTEGER},
			{Name: "country-code", Type: protomodel.FieldType_STRING},
			{Name: "address.street", Type: protomodel.FieldType_STRING},
			{Name: "active", Type: protomodel.FieldType_BOOLEAN},
		},
		[]*protomodel.Index{
			{Fields: []string{"doc-id"}, IsUnique: true},
			{Fields: []string{"number"}},
			{Fields: []string{"name"}},
			{Fields: []string{"pin"}},
			{Fields: []string{"country-code"}},
			{Fields: []string{"address.street"}},
			{Fields: []string{"active"}},
		},
	)
	require.NoError(t, err)

	// creating collection with the same name should throw error
	err = engine.CreateCollection(
		context.Background(),
		"admin",
		collectionName,
		"",
		nil,
		nil,
	)
	require.ErrorIs(t, err, ErrCollectionAlreadyExists)

	_, err = engine.GetCollection(context.Background(), "unexistentCollection")
	require.ErrorIs(t, err, ErrCollectionDoesNotExist)

	collection, err := engine.GetCollection(context.Background(), collectionName)
	require.NoError(t, err)
	require.Equal(t, collectionName, collection.Name)
	require.Len(t, collection.Fields, 7)
	require.Len(t, collection.Indexes, 7)
}

func TestListCollections(t *testing.T) {
	engine := makeEngine(t)

	collections := []string{"mycollection1", "mycollection2", "mycollection3"}

	for _, collectionName := range collections {
		err := engine.CreateCollection(
			context.Background(),
			"admin",
			collectionName,
			"",
			[]*protomodel.Field{
				{Name: "number", Type: protomodel.FieldType_INTEGER},
				{Name: "name", Type: protomodel.FieldType_STRING},
				{Name: "pin", Type: protomodel.FieldType_INTEGER},
				{Name: "country", Type: protomodel.FieldType_STRING},
				{Name: "address.street", Type: protomodel.FieldType_STRING},
			},
			[]*protomodel.Index{
				{Fields: []string{"number"}},
				{Fields: []string{"name"}},
				{Fields: []string{"pin"}},
				{Fields: []string{"country"}},
				{Fields: []string{"address.street"}},
			},
		)
		require.NoError(t, err)
	}

	collectionList, err := engine.GetCollections(context.Background())
	require.NoError(t, err)
	require.Equal(t, len(collections), len(collectionList))
}

func TestGetDocument(t *testing.T) {
	ctx := context.Background()
	engine := makeEngine(t)

	collectionName := "mycollection"

	err := engine.CreateCollection(
		context.Background(),
		"admin",
		collectionName,
		"",
		[]*protomodel.Field{
			{Name: "country", Type: protomodel.FieldType_STRING},
			{Name: "pincode", Type: protomodel.FieldType_INTEGER},
			{Name: "address.street", Type: protomodel.FieldType_STRING},
			{Name: "active", Type: protomodel.FieldType_BOOLEAN},
		},
		[]*protomodel.Index{
			{Fields: []string{"country"}},
			{Fields: []string{"pincode"}},
			{Fields: []string{"address.street"}},
			{Fields: []string{"active"}},
		},
	)
	require.NoError(t, err)

	_, _, err = engine.InsertDocument(context.Background(), "admin", "unexistentCollectionName", &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"country": structpb.NewStringValue("wonderland"),
			"pincode": structpb.NewNumberValue(2),
			"address": structpb.NewStructValue(&structpb.Struct{
				Fields: map[string]*structpb.Value{
					"street": structpb.NewStringValue("mainstreet"),
					"number": structpb.NewNumberValue(124),
				},
			}),
		},
	})
	require.ErrorIs(t, err, ErrCollectionDoesNotExist)

	_, _, err = engine.InsertDocument(context.Background(), "admin", collectionName, &structpb.Struct{
		Fields: map[string]*structpb.Value{
			DefaultDocumentIDField: structpb.NewStringValue("_docid"),
			"country":              structpb.NewStringValue("wonderland"),
			"pincode":              structpb.NewNumberValue(2),
		},
	})
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, _, err = engine.InsertDocument(context.Background(), "admin", collectionName, &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"country":         structpb.NewStringValue("wonderland"),
			"pincode":         structpb.NewNumberValue(2),
			DocumentBLOBField: structpb.NewStructValue(&structpb.Struct{}),
		},
	})
	require.ErrorIs(t, err, ErrReservedName)

	_, docID, err := engine.InsertDocument(context.Background(), "admin", collectionName, &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"country": structpb.NewStringValue("wonderland"),
			"pincode": structpb.NewNumberValue(2),
			"address": structpb.NewStructValue(&structpb.Struct{
				Fields: map[string]*structpb.Value{
					"street": structpb.NewStringValue("mainstreet"),
					"number": structpb.NewNumberValue(124),
				},
			}),
		},
	})
	require.NoError(t, err)

	query := &protomodel.Query{
		CollectionName: collectionName,
		Expressions: []*protomodel.QueryExpression{
			{
				FieldComparisons: []*protomodel.FieldComparison{
					{
						Field:    "country",
						Operator: protomodel.ComparisonOperator_EQ,
						Value:    structpb.NewStringValue("wonderland"),
					},
					{
						Field:    "pincode",
						Operator: protomodel.ComparisonOperator_EQ,
						Value:    structpb.NewNumberValue(2),
					},
					{
						Field:    "address.street",
						Operator: protomodel.ComparisonOperator_EQ,
						Value:    structpb.NewStringValue("mainstreet"),
					},
				},
			},
		},
	}

	_, err = engine.GetDocuments(ctx, nil, 0)
	require.ErrorIs(t, err, ErrIllegalArguments)

	reader, err := engine.GetDocuments(ctx, query, 0)
	require.NoError(t, err)
	defer reader.Close()

	doc, err := reader.Read(ctx)
	require.NoError(t, err)
	require.EqualValues(t, docID.EncodeToHexString(), doc.Document.Fields[DefaultDocumentIDField].GetStringValue())

	_, err = reader.Read(ctx)
	require.ErrorIs(t, err, ErrNoMoreDocuments)

	_, err = engine.CountDocuments(ctx, nil, 0)
	require.ErrorIs(t, err, ErrIllegalArguments)

	count, err := engine.CountDocuments(ctx, query, 0)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

func TestDocumentAudit(t *testing.T) {
	engine := makeEngine(t)

	collectionName := "mycollection"

	err := engine.CreateCollection(
		context.Background(),
		"admin",
		collectionName,
		"",
		[]*protomodel.Field{
			{Name: "country", Type: protomodel.FieldType_STRING},
			{Name: "pincode", Type: protomodel.FieldType_INTEGER},
			{Name: "address.street", Type: protomodel.FieldType_STRING},
		},
		[]*protomodel.Index{
			{Fields: []string{"country"}},
			{Fields: []string{"pincode"}},
			{Fields: []string{"address.street"}},
		},
	)
	require.NoError(t, err)

	// add document to collection
	txID, docID, err := engine.InsertDocument(context.Background(), "admin", collectionName, &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"country": structpb.NewStringValue("wonderland"),
			"pincode": structpb.NewNumberValue(2),
			"address": structpb.NewStructValue(&structpb.Struct{
				Fields: map[string]*structpb.Value{
					"street": structpb.NewStringValue("mainstreet"),
				},
			}),
		},
	})
	require.NoError(t, err)

	query := &protomodel.Query{
		CollectionName: collectionName,
		Expressions: []*protomodel.QueryExpression{
			{
				FieldComparisons: []*protomodel.FieldComparison{
					{
						Field:    "country",
						Operator: protomodel.ComparisonOperator_EQ,
						Value:    structpb.NewStringValue("wonderland"),
					},
					{
						Field:    "address.street",
						Operator: protomodel.ComparisonOperator_LIKE,
						Value:    structpb.NewStringValue("mainstreet"),
					},
				},
			},
		},
	}

	revisions, err := engine.ReplaceDocuments(context.Background(), "admin", query, &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"_id":     structpb.NewStringValue(docID.EncodeToHexString()),
			"pincode": structpb.NewNumberValue(2),
			"country": structpb.NewStringValue("wonderland"),
			"address": structpb.NewStructValue(&structpb.Struct{
				Fields: map[string]*structpb.Value{
					"street": structpb.NewStringValue("notmainstreet"),
				},
			}),
		},
	})
	require.NoError(t, err)
	require.Len(t, revisions, 1)
	require.Equal(t, uint64(2), revisions[0].Revision)

	err = engine.sqlEngine.GetStore().WaitForIndexingUpto(context.Background(), revisions[0].TransactionId)
	require.NoError(t, err)

	t.Run("get encoded document should pass with valid docID", func(t *testing.T) {
		_, field, doc, err := engine.GetEncodedDocument(context.Background(), collectionName, docID, 0)
		require.NoError(t, err)
		require.Equal(t, DefaultDocumentIDField, field)
		require.Equal(t, txID+1, doc.TxID)
		require.Equal(t, uint64(2), doc.Revision)
	})

	// get document audit
	res, err := engine.AuditDocument(context.Background(), collectionName, docID, false, 0, 10, true)
	require.NoError(t, err)
	require.Len(t, res, 2)

	for i, docAudit := range res {
		require.Contains(t, docAudit.Document.Fields, DefaultDocumentIDField)
		require.Contains(t, docAudit.Document.Fields, "pincode")
		require.Contains(t, docAudit.Document.Fields, "country")
		require.Contains(t, docAudit.Document.Fields, "address")
		require.NotNil(t, docAudit.Document.Fields["address"].GetStructValue())
		require.Contains(t, docAudit.Document.Fields["address"].GetStructValue().Fields, "street")
		require.Equal(t, uint64(i+1), docAudit.Revision)
	}

	err = engine.DeleteDocuments(context.Background(), "admin", &protomodel.Query{
		CollectionName: collectionName,
		Expressions: []*protomodel.QueryExpression{
			{
				FieldComparisons: []*protomodel.FieldComparison{
					{Field: "_id", Operator: protomodel.ComparisonOperator_EQ, Value: structpb.NewStringValue(docID.EncodeToHexString())},
				}},
		},
		Limit: 1,
	})
	require.NoError(t, err)

	t.Run("get encoded document should return error with deleted docID", func(t *testing.T) {
		docReader, err := engine.GetDocuments(context.Background(), &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    DefaultDocumentIDField,
							Operator: protomodel.ComparisonOperator_EQ,
							Value:    structpb.NewStringValue(docID.EncodeToHexString()),
						},
					},
				},
			},
		}, 0)
		require.NoError(t, err)
		defer docReader.Close()

		_, err = docReader.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreDocuments)
	})

	res, err = engine.AuditDocument(context.Background(), collectionName, docID, false, 0, 10, true)
	require.NoError(t, err)
	require.Len(t, res, 3)
}

func TestQueryDocuments(t *testing.T) {
	ctx := context.Background()
	engine := makeEngine(t)

	collectionName := "mycollection"

	err := engine.CreateCollection(
		context.Background(),
		"admin",
		collectionName,
		"",
		[]*protomodel.Field{
			{Name: "country", Type: protomodel.FieldType_STRING},
			{Name: "pincode", Type: protomodel.FieldType_INTEGER},
			{Name: "address.street", Type: protomodel.FieldType_STRING},
		},
		[]*protomodel.Index{
			{Fields: []string{"country"}},
			{Fields: []string{"pincode"}},
			{Fields: []string{"address.street"}},
		},
	)
	require.NoError(t, err)

	// add documents to collection
	for i := 1.0; i <= 11; i++ {
		_, _, err = engine.InsertDocument(context.Background(), "admin", collectionName, &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"pincode": func() *structpb.Value {
					if i == 11 {
						return structpb.NewNullValue()
					}
					return structpb.NewNumberValue(i)
				}(),
				"country": structpb.NewStringValue(fmt.Sprintf("country-%d", int(i))),
				"address": structpb.NewStructValue(&structpb.Struct{
					Fields: map[string]*structpb.Value{
						"street": structpb.NewStringValue(fmt.Sprintf("mainstreet-%d", int(i))),
					},
				}),
			},
		})
		require.NoError(t, err)
	}

	t.Run("test query with != operator", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_NE,
							Value:    structpb.NewNumberValue(2),
						},
						{
							Field:    "country",
							Operator: protomodel.ComparisonOperator_NOT_LIKE,
							Value:    structpb.NewStringValue("some_country"),
						},
					},
				},
			},
		}

		reader, err := engine.GetDocuments(ctx, query, 0)
		require.NoError(t, err)
		defer reader.Close()

		_, err = reader.ReadN(ctx, 0)
		require.ErrorIs(t, err, ErrIllegalArguments)

		docs, err := reader.ReadN(ctx, 11)
		require.ErrorIs(t, err, ErrNoMoreDocuments)
		require.Len(t, docs, 10)

		count, err := engine.CountDocuments(ctx, query, 0)
		require.NoError(t, err)
		require.EqualValues(t, 10, count)
	})

	t.Run("test query nested with != operator", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "address.street",
							Operator: protomodel.ComparisonOperator_NE,
							Value:    structpb.NewStringValue("mainstreet-3"),
						},
					},
				},
			},
		}

		reader, err := engine.GetDocuments(ctx, query, 0)
		require.NoError(t, err)
		defer reader.Close()

		docs, err := reader.ReadN(ctx, 11)
		require.ErrorIs(t, err, ErrNoMoreDocuments)
		require.Len(t, docs, 10)

		count, err := engine.CountDocuments(ctx, query, 0)
		require.NoError(t, err)
		require.EqualValues(t, 10, count)
	})

	t.Run("test query with < operator", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_LT,
							Value:    structpb.NewNumberValue(10),
						},
					},
				},
			},
		}

		reader, err := engine.GetDocuments(ctx, query, 0)
		require.NoError(t, err)
		defer reader.Close()

		docs, err := reader.ReadN(ctx, 11)
		require.ErrorIs(t, err, ErrNoMoreDocuments)
		require.Len(t, docs, 10)

		count, err := engine.CountDocuments(ctx, query, 0)
		require.NoError(t, err)
		require.EqualValues(t, 10, count)
	})

	t.Run("test query with <= operator", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_LE,
							Value:    structpb.NewNumberValue(9),
						},
					},
				},
			},
		}

		reader, err := engine.GetDocuments(ctx, query, 0)
		require.NoError(t, err)
		defer reader.Close()

		docs, err := reader.ReadN(ctx, 11)
		require.ErrorIs(t, err, ErrNoMoreDocuments)
		require.Len(t, docs, 10)

		count, err := engine.CountDocuments(ctx, query, 0)
		require.NoError(t, err)
		require.EqualValues(t, 10, count)
	})

	t.Run("test query with > operator", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_GT,
							Value:    structpb.NewNumberValue(5),
						},
					},
				},
			},
		}

		reader, err := engine.GetDocuments(ctx, query, 0)
		require.NoError(t, err)
		defer reader.Close()

		docs, err := reader.ReadN(ctx, 10)
		require.ErrorIs(t, err, ErrNoMoreDocuments)
		require.Len(t, docs, 5)

		count, err := engine.CountDocuments(ctx, query, 0)
		require.NoError(t, err)
		require.EqualValues(t, 5, count)
	})

	t.Run("test query with >= operator", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_GE,
							Value:    structpb.NewNumberValue(10),
						},
					},
				},
			},
		}

		reader, err := engine.GetDocuments(ctx, query, 0)
		require.NoError(t, err)
		defer reader.Close()

		docs, err := reader.ReadN(ctx, 10)
		require.ErrorIs(t, err, ErrNoMoreDocuments)
		require.Len(t, docs, 1)

		count, err := engine.CountDocuments(ctx, query, 0)
		require.NoError(t, err)
		require.EqualValues(t, 1, count)
	})

	t.Run("test group query with != operator", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "country",
							Operator: protomodel.ComparisonOperator_NE,
							Value:    structpb.NewStringValue("country-1"),
						},
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_NE,
							Value:    structpb.NewNumberValue(5),
						},
					},
				},
			},
		}

		reader, err := engine.GetDocuments(ctx, query, 0)
		require.NoError(t, err)
		defer reader.Close()

		docs, err := reader.ReadN(ctx, 10)
		require.ErrorIs(t, err, ErrNoMoreDocuments)
		require.Len(t, docs, 9)

		count, err := engine.CountDocuments(ctx, query, 0)
		require.NoError(t, err)
		require.EqualValues(t, 9, count)
	})

	t.Run("test group query with < operator", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_LT,
							Value:    structpb.NewNumberValue(5),
						},
					},
				},
			},
		}

		reader, err := engine.GetDocuments(ctx, query, 0)
		require.NoError(t, err)
		defer reader.Close()

		docs, err := reader.ReadN(ctx, 10)
		require.ErrorIs(t, err, ErrNoMoreDocuments)
		require.Len(t, docs, 5)

		count, err := engine.CountDocuments(ctx, query, 0)
		require.NoError(t, err)
		require.EqualValues(t, 5, count)
	})

	t.Run("query should fail with invalid field name", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "1invalidFieldName",
							Operator: protomodel.ComparisonOperator_LT,
							Value:    structpb.NewNumberValue(5),
						},
					},
				},
			},
		}

		_, err := engine.GetDocuments(ctx, query, 0)
		require.ErrorIs(t, err, store.ErrIllegalArguments)
	})

	t.Run("query should fail with unexistent field", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode1",
							Operator: protomodel.ComparisonOperator_LT,
							Value:    structpb.NewNumberValue(5),
						},
					},
				},
			},
		}

		_, err := engine.GetDocuments(ctx, query, 0)
		require.ErrorIs(t, err, ErrFieldDoesNotExist)
	})

	t.Run("test group query with > operator", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "country",
							Operator: protomodel.ComparisonOperator_GT,
							Value:    structpb.NewStringValue("country-1"),
						},
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_GT,
							Value:    structpb.NewNumberValue(5),
						},
					},
				},
			},
		}

		reader, err := engine.GetDocuments(ctx, query, 0)
		require.NoError(t, err)
		defer reader.Close()

		docs, err := reader.ReadN(ctx, 10)
		require.ErrorIs(t, err, ErrNoMoreDocuments)
		require.Len(t, docs, 5)

		count, err := engine.CountDocuments(ctx, query, 0)
		require.NoError(t, err)
		require.EqualValues(t, 5, count)
	})

	t.Run("test group query with IS NULL operator", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_EQ,
							Value:    structpb.NewNullValue(),
						},
					},
				},
			},
		}

		reader, err := engine.GetDocuments(ctx, query, 0)
		require.NoError(t, err)
		defer reader.Close()

		docs, err := reader.ReadN(ctx, 11)
		require.ErrorIs(t, err, ErrNoMoreDocuments)
		require.Len(t, docs, 1)

		count, err := engine.CountDocuments(ctx, query, 0)
		require.NoError(t, err)
		require.EqualValues(t, 1, count)
	})

	t.Run("test group query with IS NOT NULL operator", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_NE,
							Value:    structpb.NewNullValue(),
						},
					},
				},
			},
		}

		reader, err := engine.GetDocuments(ctx, query, 0)
		require.NoError(t, err)
		defer reader.Close()

		docs, err := reader.ReadN(ctx, 11)
		require.ErrorIs(t, err, ErrNoMoreDocuments)
		require.Len(t, docs, 10)

		count, err := engine.CountDocuments(ctx, query, 0)
		require.NoError(t, err)
		require.EqualValues(t, 10, count)
	})
}

func TestDocumentUpdate(t *testing.T) {
	// Create a new engine instance
	ctx := context.Background()
	engine := makeEngine(t)

	// Create a test collection with a single document
	collectionName := "test_collection"

	err := engine.CreateCollection(
		context.Background(),
		"admin",
		collectionName,
		"",
		[]*protomodel.Field{
			{Name: "name", Type: protomodel.FieldType_STRING},
			{Name: "age", Type: protomodel.FieldType_DOUBLE},
		},
		[]*protomodel.Index{
			{Fields: []string{"name"}},
			{Fields: []string{"age"}},
		},
	)
	require.NoError(t, err)

	txID, docID, err := engine.InsertDocument(context.Background(), "admin", collectionName, &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"name": structpb.NewStringValue("Alice"),
			"age":  structpb.NewNumberValue(30),
		},
	})
	require.NoError(t, err)

	t.Run("update document should pass without docID", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "name",
							Operator: protomodel.ComparisonOperator_EQ,
							Value:    structpb.NewStringValue("Alice"),
						},
						{
							Field:    "age",
							Operator: protomodel.ComparisonOperator_EQ,
							Value:    structpb.NewNumberValue(30),
						},
					},
				},
			},
		}

		revisions, err := engine.ReplaceDocuments(ctx, "admin", query, &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"name": structpb.NewStringValue("Alice"),
				"age":  structpb.NewNumberValue(31),
			},
		})
		require.NoError(t, err)
		// Check that the method returned the expected values
		require.Len(t, revisions, 1)

		require.Equal(t, txID+1, revisions[0].TransactionId)
		require.Equal(t, docID.EncodeToHexString(), revisions[0].DocumentId)
		require.EqualValues(t, 2, revisions[0].Revision)

		// Verify that the document was updated
		query = &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "name",
							Operator: protomodel.ComparisonOperator_EQ,
							Value:    structpb.NewStringValue("Alice"),
						},
						{
							Field:    "age",
							Operator: protomodel.ComparisonOperator_EQ,
							Value:    structpb.NewNumberValue(31),
						},
					},
				},
			},
		}

		reader, err := engine.GetDocuments(ctx, query, 0)
		require.NoError(t, err)
		defer reader.Close()

		updatedDoc, err := reader.Read(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 31, updatedDoc.Document.Fields["age"].GetNumberValue())
		require.Equal(t, docID.EncodeToHexString(), updatedDoc.Document.Fields[DefaultDocumentIDField].GetStringValue())
	})

	t.Run("update document should fail when no document is found", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "name",
							Operator: protomodel.ComparisonOperator_EQ,
							Value:    structpb.NewStringValue("Bob"),
						},
					},
				},
			},
		}

		toUpdateDoc := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"name": structpb.NewStringValue("Alice"),
				"age":  structpb.NewNumberValue(32),
			},
		}

		// Test error case when no documents are found
		revisions, err := engine.ReplaceDocuments(ctx, "admin", query, toUpdateDoc)
		require.NoError(t, err)
		require.Empty(t, revisions)
	})

	t.Run("update document should fail with a different docID", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "name",
							Operator: protomodel.ComparisonOperator_EQ,
							Value:    structpb.NewStringValue("Alice"),
						},
					},
				},
			},
		}

		toUpdateDoc := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				DefaultDocumentIDField: structpb.NewStringValue("1234"),
				"name":                 structpb.NewStringValue("Alice"),
				"age":                  structpb.NewNumberValue(31),
			},
		}

		revisions, err := engine.ReplaceDocuments(ctx, "admin", query, toUpdateDoc)
		require.NoError(t, err)
		require.Empty(t, revisions)
	})

	t.Run("replace document with invalid arguments should fail", func(t *testing.T) {
		_, err := engine.ReplaceDocuments(ctx, "admin", nil, nil)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("replace document with invalid collection name should fail", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: "1invalidCollectionName",
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "name",
							Operator: protomodel.ComparisonOperator_EQ,
							Value:    structpb.NewStringValue("Alice"),
						},
					},
				},
			},
		}

		toUpdateDoc := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				DefaultDocumentIDField: structpb.NewStringValue("1234"),
				"name":                 structpb.NewStringValue("Alice"),
				"age":                  structpb.NewNumberValue(31),
			},
		}

		_, err := engine.ReplaceDocuments(ctx, "admin", query, toUpdateDoc)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("replace document with empty document should succeed", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "age",
							Operator: protomodel.ComparisonOperator_EQ,
							Value:    structpb.NewNumberValue(31),
						},
					},
				},
			},
		}

		revisions, err := engine.ReplaceDocuments(ctx, "admin", query, nil)
		require.NoError(t, err)
		require.Len(t, revisions, 1)
	})

	t.Run("replace document with query without expressions should succeed", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions:    []*protomodel.QueryExpression{},
		}

		toUpdateDoc := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				DefaultDocumentIDField: structpb.NewStringValue(docID.EncodeToHexString()),
				"name":                 structpb.NewStringValue("Alice"),
				"age":                  structpb.NewNumberValue(32),
			},
		}

		revisions, err := engine.ReplaceDocuments(ctx, "admin", query, toUpdateDoc)
		require.NoError(t, err)
		require.Len(t, revisions, 1)
	})
}
func TestCustomDocumentID(t *testing.T) {
	// Create a new engine instance
	ctx := context.Background()
	engine := makeEngine(t)

	// Create a test collection with a single document
	collectionName := "test_collection"
	customIDField := "customID"

	err := engine.CreateCollection(
		context.Background(),
		"admin",
		collectionName,
		"customID",
		[]*protomodel.Field{
			{Name: "name", Type: protomodel.FieldType_STRING},
			{Name: "age", Type: protomodel.FieldType_DOUBLE},
		},
		[]*protomodel.Index{},
	)
	require.NoError(t, err)

	t.Run("insert document with custom doc ID should pass", func(t *testing.T) {
		_, docID, err := engine.InsertDocument(context.Background(), "admin", collectionName, &structpb.Struct{
			Fields: map[string]*structpb.Value{
				customIDField: structpb.NewStringValue("1234"),
				"name":        structpb.NewStringValue("Alice"),
				"age":         structpb.NewNumberValue(30),
			},
		})
		require.NoError(t, err)
		require.Equal(t, "1234", docID.EncodeToHexString())

		// query document
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{{
				FieldComparisons: []*protomodel.FieldComparison{{
					Field: customIDField, Operator: protomodel.ComparisonOperator_EQ, Value: structpb.NewStringValue("1234"),
				}},
			}},
		}

		reader, err := engine.GetDocuments(ctx, query, 0)
		require.NoError(t, err)
		defer reader.Close()

		doc, err := reader.Read(ctx)
		require.NoError(t, err)
		// require.Equal(t, txID, doc.)
		require.Equal(t, "1234", doc.Document.Fields[customIDField].GetStringValue())
	})

	t.Run("update document with custom doc ID", func(t *testing.T) {
		txID, _, err := engine.InsertDocument(context.Background(), "admin", collectionName, &structpb.Struct{
			Fields: map[string]*structpb.Value{
				customIDField: structpb.NewStringValue("1235"),
				"name":        structpb.NewStringValue("Bob"),
				"age":         structpb.NewNumberValue(50),
			},
		})
		require.NoError(t, err)

		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{{
				FieldComparisons: []*protomodel.FieldComparison{{
					Field: customIDField, Operator: protomodel.ComparisonOperator_EQ, Value: structpb.NewStringValue("1235"),
				}},
			}},
		}

		docs, err := engine.ReplaceDocuments(context.Background(), "admin", query, &structpb.Struct{
			Fields: map[string]*structpb.Value{
				customIDField: structpb.NewStringValue("1235"),
				"age":         structpb.NewNumberValue(51),
			},
		})
		require.NoError(t, err)
		require.Len(t, docs, 1)
		require.Equal(t, txID+1, docs[0].TransactionId)
		require.Equal(t, "1235", docs[0].DocumentId)
	})

	t.Run("insert document not hex should fail", func(t *testing.T) {
		_, _, err := engine.InsertDocument(context.Background(), "admin", collectionName, &structpb.Struct{
			Fields: map[string]*structpb.Value{
				customIDField: structpb.NewStringValue("test_not_hex"),
				"name":        structpb.NewStringValue("Alice"),
				"age":         structpb.NewNumberValue(30),
			},
		})
		require.Error(t, err, ErrInvalidHex)
	})
}

func TestFloatSupport(t *testing.T) {
	ctx := context.Background()
	engine := makeEngine(t)

	collectionName := "mycollection"

	err := engine.CreateCollection(
		context.Background(),
		"admin",
		collectionName,
		"",
		[]*protomodel.Field{
			{Name: "number", Type: protomodel.FieldType_DOUBLE},
		},
		[]*protomodel.Index{
			{Fields: []string{"number"}},
		},
	)
	require.NoError(t, err)

	// add document to collection
	_, _, err = engine.InsertDocument(context.Background(), "admin", collectionName, &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"number": structpb.NewNumberValue(3.1),
		},
	})
	require.NoError(t, err)

	// query document
	query := &protomodel.Query{
		CollectionName: collectionName,
		Expressions: []*protomodel.QueryExpression{
			{
				FieldComparisons: []*protomodel.FieldComparison{
					{
						Field:    "number",
						Operator: protomodel.ComparisonOperator_EQ,
						Value:    structpb.NewNumberValue(3.1),
					},
				},
			},
		},
	}

	// check if document is updated
	reader, err := engine.GetDocuments(ctx, query, 0)
	require.NoError(t, err)
	defer reader.Close()

	doc, err := reader.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, 3.1, doc.Document.Fields["number"].GetNumberValue())
}

func TestDeleteCollection(t *testing.T) {
	engine := makeEngine(t)

	// create collection
	collectionName := "mycollection"

	err := engine.CreateCollection(
		context.Background(),
		"admin",
		collectionName,
		"",
		[]*protomodel.Field{
			{Name: "number", Type: protomodel.FieldType_INTEGER},
		},
		[]*protomodel.Index{
			{Fields: []string{"number"}},
		},
	)
	require.NoError(t, err)

	// add documents to collection
	for i := 1.0; i <= 10; i++ {
		_, _, err = engine.InsertDocument(context.Background(), "admin", collectionName, &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"number": structpb.NewNumberValue(i),
			},
		})
		require.NoError(t, err)
	}

	t.Run("delete collection and check if it is empty", func(t *testing.T) {
		err = engine.DeleteCollection(context.Background(), "admin", collectionName)
		require.NoError(t, err)

		collectionList, err := engine.GetCollections(context.Background())
		require.NoError(t, err)
		require.Empty(t, collectionList)
	})
}

func TestUpdateCollection(t *testing.T) {
	engine := makeEngine(t)

	collectionName := "mycollection"

	t.Run("create collection and add index", func(t *testing.T) {
		err := engine.CreateCollection(
			context.Background(),
			"admin",
			collectionName,
			"",
			[]*protomodel.Field{
				{Name: "number", Type: protomodel.FieldType_DOUBLE},
				{Name: "name", Type: protomodel.FieldType_STRING},
				{Name: "country", Type: protomodel.FieldType_STRING},
				{Name: "pin", Type: protomodel.FieldType_INTEGER},
			},
			[]*protomodel.Index{
				{Fields: []string{"number"}},
				{Fields: []string{"name"}},
				{Fields: []string{"country"}},
				{Fields: []string{"pin"}},
			},
		)
		require.NoError(t, err)
	})

	t.Run("update collection should fail with invalid collection name", func(t *testing.T) {
		// update collection
		err := engine.UpdateCollection(
			context.Background(),
			"admin",
			"1invalidCollectionName",
			"",
		)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("update collection should fail with unexistent collection name", func(t *testing.T) {
		// update collection
		err := engine.UpdateCollection(
			context.Background(),
			"admin",
			"unexistentCollectionName",
			"",
		)
		require.ErrorIs(t, err, ErrCollectionDoesNotExist)
	})

	t.Run("update collection should fail with invalid id field name", func(t *testing.T) {
		// update collection
		err := engine.UpdateCollection(
			context.Background(),
			"admin",
			collectionName,
			"document",
		)
		require.ErrorIs(t, err, ErrReservedName)
	})

	t.Run("update collection by deleting indexes", func(t *testing.T) {
		// update collection
		err := engine.UpdateCollection(
			context.Background(),
			"admin",
			collectionName,
			"",
		)
		require.NoError(t, err)

		// get collection
		collection, err := engine.GetCollection(context.Background(), collectionName)
		require.NoError(t, err)
		require.Equal(t, DefaultDocumentIDField, collection.DocumentIdFieldName)
		require.Len(t, collection.Indexes, 5)
	})

	t.Run("update collection by adding changing documentIdFieldName", func(t *testing.T) {
		// update collection
		err := engine.UpdateCollection(
			context.Background(),
			"admin",
			collectionName,
			"_docid",
		)
		require.NoError(t, err)

		// get collection
		collection, err := engine.GetCollection(context.Background(), collectionName)
		require.NoError(t, err)
		require.Equal(t, "_docid", collection.DocumentIdFieldName)
		require.Len(t, collection.Indexes, 5)
	})

	t.Run("update collection with invalid id field name", func(t *testing.T) {
		err := engine.UpdateCollection(
			context.Background(),
			"admin",
			collectionName,
			"document",
		)
		require.ErrorIs(t, err, ErrReservedName)
	})
}

func TestCollectionUpdateWithDeletedIndex(t *testing.T) {
	engine := makeEngine(t)

	collectionName := "mycollection"

	t.Run("create collection and add index", func(t *testing.T) {
		err := engine.CreateCollection(
			context.Background(),
			"admin",
			collectionName,
			"",
			[]*protomodel.Field{
				{Name: "number", Type: protomodel.FieldType_DOUBLE},
				{Name: "title", Type: protomodel.FieldType_STRING},
				{Name: "comment", Type: protomodel.FieldType_STRING},
			},
			[]*protomodel.Index{
				{Fields: []string{"number"}},
			},
		)
		require.NoError(t, err)
	})

	t.Run("create index with invalid collection name should fail", func(t *testing.T) {
		// update collection
		err := engine.CreateIndex(
			context.Background(),
			"admin",
			"1invalidCollectionName",
			[]string{},
			false,
		)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("create index with no fields should fail", func(t *testing.T) {
		// update collection
		err := engine.CreateIndex(
			context.Background(),
			"admin",
			collectionName,
			[]string{},
			false,
		)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("create index with invalid field name should fail", func(t *testing.T) {
		// update collection
		err := engine.CreateIndex(
			context.Background(),
			"admin",
			collectionName,
			[]string{"1invalidFieldName"},
			false,
		)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("create index with unexistent field name should fail", func(t *testing.T) {
		// update collection
		err := engine.CreateIndex(
			context.Background(),
			"admin",
			collectionName,
			[]string{"unexistentFieldName"},
			false,
		)
		require.ErrorIs(t, err, ErrFieldDoesNotExist)

		err = engine.RemoveField(
			context.Background(),
			"admin",
			"1invalidCollectionName",
			"comment",
		)
		require.ErrorIs(t, err, ErrIllegalArguments)

		err = engine.RemoveField(
			context.Background(),
			"admin",
			collectionName,
			"1invalidFieldName",
		)
		require.ErrorIs(t, err, ErrIllegalArguments)

		err = engine.RemoveField(
			context.Background(),
			"admin",
			collectionName,
			"unexistentFieldName",
		)
		require.ErrorIs(t, err, ErrFieldDoesNotExist)
	})

	t.Run("adding invalid field should fail", func(t *testing.T) {
		err := engine.AddField(
			context.Background(),
			"admin",
			"1invalidCollectionName",
			&protomodel.Field{
				Name: "newFieldName",
				Type: protomodel.FieldType_INTEGER,
			},
		)
		require.ErrorIs(t, err, ErrIllegalArguments)

		err = engine.AddField(
			context.Background(),
			"admin",
			collectionName,
			&protomodel.Field{
				Name: "1invalidFieldName",
				Type: protomodel.FieldType_INTEGER,
			},
		)
		require.ErrorIs(t, err, ErrIllegalArguments)

		err = engine.AddField(
			context.Background(),
			"admin",
			collectionName,
			&protomodel.Field{
				Name: "newFieldName",
				Type: protomodel.FieldType(math.MaxInt16),
			},
		)
		require.ErrorIs(t, err, ErrUnsupportedType)
	})

	t.Run("removing invalid field should fail", func(t *testing.T) {
		err := engine.AddField(
			context.Background(),
			"admin",
			"1invalidCollectionName",
			&protomodel.Field{
				Name: "newFieldName",
				Type: protomodel.FieldType_INTEGER,
			},
		)
		require.ErrorIs(t, err, ErrIllegalArguments)

		err = engine.AddField(
			context.Background(),
			"admin",
			collectionName,
			&protomodel.Field{
				Name: "1invalidFieldName",
				Type: protomodel.FieldType_INTEGER,
			},
		)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("create index with a new field name should succeed", func(t *testing.T) {
		_, _, err := engine.InsertDocument(
			context.Background(),
			"admin",
			collectionName,
			&structpb.Struct{
				Fields: map[string]*structpb.Value{
					"number":  structpb.NewNumberValue(1),
					"title":   structpb.NewStringValue("title1"),
					"comment": structpb.NewStringValue("some comment"),
				},
			})
		require.NoError(t, err)

		err = engine.RemoveField(
			context.Background(),
			"admin",
			collectionName,
			"title",
		)
		require.NoError(t, err)

		err = engine.AddField(
			context.Background(),
			"admin",
			collectionName,
			&protomodel.Field{
				Name: "active",
				Type: protomodel.FieldType_BOOLEAN,
			},
		)
		require.NoError(t, err)

		err = engine.AddField(
			context.Background(),
			"admin",
			collectionName,
			&protomodel.Field{
				Name: "active",
				Type: protomodel.FieldType_BOOLEAN,
			},
		)
		require.ErrorIs(t, err, ErrFieldAlreadyExists)

		err = engine.CreateIndex(
			context.Background(),
			"admin",
			collectionName,
			[]string{"active"},
			false,
		)
		require.NoError(t, err)

		_, _, err = engine.InsertDocument(
			context.Background(),
			"admin",
			collectionName,
			&structpb.Struct{
				Fields: map[string]*structpb.Value{
					"number":  structpb.NewNumberValue(1),
					"title":   structpb.NewStringValue("title1"),
					"comment": structpb.NewStringValue("some comment"),
				},
			})
		require.NoError(t, err)

		err = engine.RemoveField(
			context.Background(),
			"admin",
			collectionName,
			"active",
		)
		require.ErrorIs(t, err, sql.ErrCannotDropColumn)

		err = engine.DeleteIndex(
			context.Background(),
			"admin",
			collectionName,
			[]string{"active"},
		)
		require.NoError(t, err)

		err = engine.RemoveField(
			context.Background(),
			"admin",
			collectionName,
			"active",
		)
		require.NoError(t, err)

		err = engine.RemoveField(
			context.Background(),
			"admin",
			collectionName,
			"active",
		)
		require.ErrorIs(t, err, ErrFieldDoesNotExist)
	})

	t.Run("delete index with invalid collection name should fail", func(t *testing.T) {
		// update collection
		err := engine.DeleteIndex(
			context.Background(),
			"admin",
			"1invalidCollectionName",
			[]string{"number"},
		)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("delete index without fields should fail", func(t *testing.T) {
		// update collection
		err := engine.DeleteIndex(
			context.Background(),
			"admin",
			collectionName,
			[]string{},
		)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("delete index with invalid field name should fail", func(t *testing.T) {
		// update collection
		err := engine.DeleteIndex(
			context.Background(),
			"admin",
			collectionName,
			[]string{"1invalidFieldName"},
		)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("update collection by deleting indexes", func(t *testing.T) {
		// update collection
		err := engine.DeleteIndex(
			context.Background(),
			"admin",
			collectionName,
			[]string{"number"},
		)
		require.NoError(t, err)

		// get collection
		collection, err := engine.GetCollection(context.Background(), collectionName)
		require.NoError(t, err)
		require.Len(t, collection.Indexes, 1)
	})

	t.Run("update collection by adding the same index should pass", func(t *testing.T) {
		// update collection
		err := engine.CreateIndex(
			context.Background(),
			"admin",
			collectionName,
			[]string{"number"},
			false,
		)
		require.NoError(t, err)

		// get collection
		collection, err := engine.GetCollection(context.Background(), collectionName)
		require.NoError(t, err)
		require.Len(t, collection.Indexes, 2)
	})
}

func TestBulkInsert(t *testing.T) {
	ctx := context.Background()
	engine := makeEngine(t)

	// create collection
	collectionName := "mycollection"

	err := engine.CreateCollection(
		context.Background(),
		"admin",
		collectionName,
		"",
		[]*protomodel.Field{
			{Name: "country", Type: protomodel.FieldType_STRING},
			{Name: "price", Type: protomodel.FieldType_DOUBLE},
		},
		[]*protomodel.Index{
			{Fields: []string{"country"}},
			{Fields: []string{"price"}},
		},
	)
	require.NoError(t, err)

	// add documents to collection
	docs := make([]*structpb.Struct, 0)

	for i := 1.0; i <= 10; i++ {
		doc := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"country": structpb.NewStringValue(fmt.Sprintf("country-%d", int(i))),
				"price":   structpb.NewNumberValue(i),
			},
		}
		docs = append(docs, doc)
	}

	txID, docIDs, err := engine.InsertDocuments(ctx, "admin", collectionName, docs)
	require.NoError(t, err)
	require.Equal(t, uint64(2), txID)
	require.Len(t, docIDs, 10)

	reader, err := engine.GetDocuments(ctx, &protomodel.Query{CollectionName: collectionName}, 0)
	require.NoError(t, err)
	defer reader.Close()

	res, err := reader.ReadN(ctx, 10)
	require.NoError(t, err)
	require.Len(t, docs, 10)

	for i, doc := range res {
		require.Equal(t, float64(i+1), doc.Document.Fields["price"].GetNumberValue())
	}
}

func TestPaginationOnReader(t *testing.T) {
	ctx := context.Background()
	engine := makeEngine(t)

	// create collection
	collectionName := "mycollection"

	err := engine.CreateCollection(
		context.Background(),
		"admin",
		collectionName,
		"",
		[]*protomodel.Field{
			{Name: "country", Type: protomodel.FieldType_STRING},
			{Name: "pincode", Type: protomodel.FieldType_INTEGER},
		},
		[]*protomodel.Index{
			{Fields: []string{"country"}},
			{Fields: []string{"pincode"}},
		},
	)
	require.NoError(t, err)

	// add documents to collection
	for i := 1.0; i <= 20; i++ {
		_, _, err = engine.InsertDocument(ctx, "admin", collectionName, &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"country": structpb.NewStringValue(fmt.Sprintf("country-%d", int(i))),
				"pincode": structpb.NewNumberValue(i),
			},
		})
		require.NoError(t, err)
	}

	t.Run("test reader for multiple reads", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_GE,
							Value:    structpb.NewNumberValue(0),
						},
					},
				},
			},
		}

		reader, err := engine.GetDocuments(ctx, query, 0)
		require.NoError(t, err)
		defer reader.Close()

		results := make([]*protomodel.DocumentAtRevision, 0)
		// use the reader to read paginated documents 5 at a time
		for i := 0; i < 4; i++ {
			docs, err := reader.ReadN(ctx, 5)
			require.NoError(t, err)
			require.Len(t, docs, 5)
			results = append(results, docs...)
		}

		for i := 1.0; i <= 20; i++ {
			doc := results[int(i-1)]
			require.Equal(t, i, doc.Document.Fields["pincode"].GetNumberValue())
		}
	})
}

func TestDeleteDocument(t *testing.T) {
	ctx := context.Background()
	engine := makeEngine(t)
	// create collection
	collectionName := "mycollection"
	err := engine.CreateCollection(context.Background(), "admin", collectionName, "", []*protomodel.Field{
		{Name: "pincode", Type: protomodel.FieldType_INTEGER},
		{Name: "country", Type: protomodel.FieldType_STRING},
	}, nil)
	require.NoError(t, err)

	// add document to collection
	_, _, err = engine.InsertDocument(context.Background(), "admin", collectionName, &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"pincode": structpb.NewNumberValue(2),
			"country": structpb.NewStringValue("wonderland"),
		},
	})
	require.NoError(t, err)

	query := &protomodel.Query{
		CollectionName: collectionName,
		Expressions: []*protomodel.QueryExpression{
			{
				FieldComparisons: []*protomodel.FieldComparison{
					{
						Field:    "country",
						Operator: protomodel.ComparisonOperator_EQ,
						Value:    structpb.NewStringValue("wonderland"),
					},
					{
						Field:    "pincode",
						Operator: protomodel.ComparisonOperator_EQ,
						Value:    structpb.NewNumberValue(2),
					},
				},
			},
		},
		Limit: 1,
	}

	reader, err := engine.GetDocuments(ctx, query, 0)
	require.NoError(t, err)
	defer reader.Close()

	docs, err := reader.ReadN(ctx, 1)
	require.NoError(t, err)
	require.Len(t, docs, 1)

	err = engine.DeleteDocuments(ctx, "admin", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = engine.DeleteDocuments(ctx, "admin", query)
	require.NoError(t, err)

	reader, err = engine.GetDocuments(ctx, query, 0)
	require.NoError(t, err)
	defer reader.Close()

	_, err = reader.Read(ctx)
	require.ErrorIs(t, ErrNoMoreDocuments, err)
}

func TestGetCollection(t *testing.T) {
	engine := makeEngine(t)
	collectionName := "mycollection1"

	err := engine.CreateCollection(
		context.Background(),
		"admin",
		collectionName,
		"",
		[]*protomodel.Field{
			{Name: "number", Type: protomodel.FieldType_INTEGER},
			{Name: "name", Type: protomodel.FieldType_STRING},
			{Name: "pin", Type: protomodel.FieldType_INTEGER},
			{Name: "country", Type: protomodel.FieldType_STRING},
		},
		[]*protomodel.Index{
			{Fields: []string{"number"}},
			{Fields: []string{"name"}},
			{Fields: []string{"pin"}},
			{Fields: []string{"country"}},
		},
	)
	require.NoError(t, err)

	collection, err := engine.GetCollection(context.Background(), collectionName)
	require.NoError(t, err)
	require.Equal(t, collectionName, collection.Name)
	require.Len(t, collection.Fields, 5)
	require.Len(t, collection.Indexes, 5)

	expectedIndexKeys := []*protomodel.Field{
		{Name: "_id", Type: protomodel.FieldType_STRING},
		{Name: "number", Type: protomodel.FieldType_INTEGER},
		{Name: "name", Type: protomodel.FieldType_STRING},
		{Name: "pin", Type: protomodel.FieldType_INTEGER},
		{Name: "country", Type: protomodel.FieldType_STRING},
	}

	for i, idxType := range expectedIndexKeys {
		require.Equal(t, idxType.Name, collection.Fields[i].Name)
		require.Equal(t, idxType.Type, collection.Fields[i].Type)
	}
}

func TestGetDocuments_WithOrderBy(t *testing.T) {
	ctx := context.Background()
	engine := makeEngine(t)

	collectionName := "mycollection"

	err := engine.CreateCollection(
		context.Background(),
		"admin",
		collectionName,
		"",
		[]*protomodel.Field{
			{Name: "number", Type: protomodel.FieldType_DOUBLE},
			{Name: "age", Type: protomodel.FieldType_DOUBLE},
		},
		[]*protomodel.Index{
			{Fields: []string{"number", "age"}},
		},
	)
	require.NoError(t, err)

	noOfDocs := 5

	for i := 1; i <= noOfDocs; i++ {
		_, _, err = engine.InsertDocument(context.Background(), "admin", collectionName, &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"number": structpb.NewNumberValue(float64(i)),
			},
		})
		require.NoError(t, err)
	}

	t.Run("order by single field", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "number",
							Operator: protomodel.ComparisonOperator_LE,
							Value:    structpb.NewNumberValue(5),
						},
					},
				},
			},
			OrderBy: []*protomodel.OrderByClause{{
				Field: "number",
				Desc:  true,
			}},
		}

		reader, err := engine.GetDocuments(ctx, query, 0)
		require.NoError(t, err)
		defer reader.Close()

		docs, err := reader.ReadN(ctx, noOfDocs)
		require.NoError(t, err)
		require.Len(t, docs, 5)

		i := noOfDocs
		for _, doc := range docs {
			require.Equal(t, float64(i), doc.Document.Fields["number"].GetNumberValue())
			i--
		}
	})
}

func BenchmarkInsertion(b *testing.B) {
	stOpts := store.DefaultOptions().
		WithMultiIndexing(true).
		WithMaxConcurrency(100)

	st, err := store.Open(b.TempDir(), stOpts)
	require.NoError(b, err)

	defer func() {
		err := st.Close()
		if !b.Failed() {
			// Do not pollute error output if test has already failed
			require.NoError(b, err)
		}
	}()

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(docPrefix))
	require.NoError(b, err)

	collectionName := "mycollection"

	err = engine.CreateCollection(
		context.Background(),
		"admin",
		collectionName,
		"",
		[]*protomodel.Field{
			{Name: "number", Type: protomodel.FieldType_DOUBLE},
			{Name: "age", Type: protomodel.FieldType_DOUBLE},
		},
		[]*protomodel.Index{
			{Fields: []string{"number", "age"}},
		},
	)
	require.NoError(b, err)

	b.ResetTimer()

	noOfWorkers := 100
	noOfDocs := 10

	for it := 0; it < 1; it++ {
		var wg sync.WaitGroup
		wg.Add(noOfWorkers)

		for w := 0; w < noOfWorkers; w++ {
			go func(w int) {
				for i := 1; i <= noOfDocs; i++ {
					_, _, err = engine.InsertDocument(context.Background(), "admin", collectionName, &structpb.Struct{
						Fields: map[string]*structpb.Value{
							"number": structpb.NewNumberValue(float64(w*noOfDocs + i)),
						},
					})
					if err != nil {
						b.Fail()
					}
				}
				wg.Done()
			}(w)
		}

		wg.Wait()
	}
}
