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

package client

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenSevice_setToken(t *testing.T) {
	fn := "deleteme"
	ts := tokenService{tokenFileName: fn, hds: NewHomedirService()}
	err := ts.SetToken("db1", "toooooken")
	assert.Nil(t, err)
	database, err := ts.GetDatabase()
	assert.Nil(t, err)
	assert.Equal(t, "db1", database)
	token, err := ts.GetToken()
	assert.Nil(t, err)
	assert.Equal(t, "toooooken", token)
	os.Remove(fn)
}

func TestTokenService_IsTokenPresent(t *testing.T) {
	fn := "deleteme"
	ts := tokenService{tokenFileName: fn, hds: NewHomedirService()}
	err := ts.SetToken("db1", "toooooken")
	require.Nil(t, err)
	ok, err := ts.IsTokenPresent()
	require.Nil(t, err)
	assert.True(t, ok)
}

func TestTokenService_DeleteToken(t *testing.T) {
	fn := "deleteme"
	ts := tokenService{tokenFileName: fn, hds: NewHomedirService()}
	err := ts.SetToken("db1", "toooooken")
	require.Nil(t, err)
	err = ts.DeleteToken()
	assert.Nil(t, err)
}
