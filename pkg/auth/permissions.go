/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package auth

// PermissionSysAdmin the admin permission byte
const PermissionSysAdmin = 255

// PermissionAdmin the system admin permission byte
const PermissionAdmin = 254

// Non-admin permissions
const (
	PermissionNone = iota
	PermissionR
	PermissionRW
)

var methodsPermissions = map[string][]uint32{
	// readwrite methods
	"Set":                    {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"VerifiableSet":          {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"StreamSet":              {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"StreamVerifiableSet":    {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"Get":                    {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"VerifiableGet":          {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"StreamGet":              {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"StreamVerifiableGet":    {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"GetAll":                 {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"ExecAll":                {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"StreamExecAll":          {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"SetReference":           {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"VerifiableSetReference": {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"ZAdd":                   {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"VerifiableZAdd":         {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"ZScan":                  {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"StreamZScan":            {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"VerifiableTxByID":       {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"IScan":                  {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"Scan":                   {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"StreamScan":             {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"History":                {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"StreamHistory":          {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"TxByID":                 {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"TxScan":                 {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"Count":                  {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"CountAll":               {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"DatabaseList":           {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},
	"CurrentState":           {PermissionSysAdmin, PermissionAdmin, PermissionRW, PermissionR},

	// admin methods
	"ListUsers":        {PermissionSysAdmin, PermissionAdmin},
	"CreateUser":       {PermissionSysAdmin, PermissionAdmin},
	"ChangePassword":   {PermissionSysAdmin, PermissionAdmin},
	"SetPermission":    {PermissionSysAdmin, PermissionAdmin},
	"DeactivateUser":   {PermissionSysAdmin, PermissionAdmin},
	"SetActiveUser":    {PermissionSysAdmin, PermissionAdmin},
	"UpdateAuthConfig": {PermissionSysAdmin},
	"UpdateMTLSConfig": {PermissionSysAdmin},
	"CreateDatabase":   {PermissionSysAdmin},
	"Dump":             {PermissionSysAdmin, PermissionAdmin},
	"CleanIndex":       {PermissionSysAdmin, PermissionAdmin},
}

//HasPermissionForMethod checks if userPermission can access method name
func HasPermissionForMethod(userPermission uint32, method string) bool {
	methodPermissions, ok := methodsPermissions[method]
	if !ok {
		return false
	}
	for _, val := range methodPermissions {
		if val == userPermission {
			return true
		}
	}
	return false
}
