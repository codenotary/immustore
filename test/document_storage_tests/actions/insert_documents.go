/*
Copyright 2023 Codenotary Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package actions

import (
	"encoding/json"
	"net/http"

	"github.com/gavv/httpexpect/v2"
)

func InsertDocumentWithName(expect *httpexpect.Expect, token string, name string) *httpexpect.Object {
	payloadModel := `{
		"name": "string"
	}`
	var payload map[string]interface{}
	json.Unmarshal([]byte(payloadModel), &payload)
	payload["name"] = name

	expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", token).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).JSON().Object().NotEmpty()

	collection := expect.GET("/collections/get").
		WithHeader("grpc-metadata-sessionid", token).
		WithQuery("name", payload["name"]).
		Expect().
		Status(http.StatusOK).
		JSON().Object()

	return collection
}
