/*
 * immudb REST API v2
 *
 * No description provided (generated by Swagger Codegen https://github.com/swagger-api/swagger-codegen)
 *
 * API version: version not set
 * Generated by: Swagger Codegen (https://github.com/swagger-api/swagger-codegen.git)
 */

package swagger

type Schemav2DocumentSearchRequest struct {
	Collection string `json:"collection,omitempty"`
	Query []Schemav2DocumentQuery `json:"query,omitempty"`
	Page int64 `json:"page,omitempty"`
	PerPage int64 `json:"perPage,omitempty"`
}
