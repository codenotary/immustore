/*
Copyright 2019-2020 vChain, Inc.

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

package immutest

import (
	"context"
	"errors"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

type homedirServiceMock struct {
	WriteFileToUserHomeDirF    func(content []byte, pathToFile string) error
	FileExistsInUserHomeDirF   func(pathToFile string) (bool, error)
	ReadFileFromUserHomeDirF   func(pathToFile string) (string, error)
	DeleteFileFromUserHomeDirF func(pathToFile string) error
}

func (hsm *homedirServiceMock) WriteFileToUserHomeDir(content []byte, pathToFile string) error {
	return hsm.WriteFileToUserHomeDirF(content, pathToFile)
}
func (hsm *homedirServiceMock) FileExistsInUserHomeDir(pathToFile string) (bool, error) {
	return hsm.FileExistsInUserHomeDirF(pathToFile)
}
func (hsm *homedirServiceMock) ReadFileFromUserHomeDir(pathToFile string) (string, error) {
	return hsm.ReadFileFromUserHomeDirF(pathToFile)
}
func (hsm *homedirServiceMock) DeleteFileFromUserHomeDir(pathToFile string) error {
	return hsm.DeleteFileFromUserHomeDirF(pathToFile)
}

type pwrMock struct {
	readF func(msg string) ([]byte, error)
}

func (pr *pwrMock) Read(msg string) ([]byte, error) {
	return pr.readF(msg)
}

type trMock struct {
	ReadFromTerminalYNF func(def string) (selected string, err error)
}

func (tr *trMock) ReadFromTerminalYN(def string) (selected string, err error) {
	return tr.ReadFromTerminalYNF(def)
}

func TestImmutest(t *testing.T) {
	viper.Set("database", "defaultdb")
	viper.Set("user", "immudb")
	data := map[string]string{}
	var index uint64
	loginFOK := func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		return &schema.LoginResponse{Token: []byte("token")}, nil
	}
	disconnectFOK := func() error { return nil }
	useDatabaseFOK := func(ctx context.Context, d *schema.Database) (*schema.UseDatabaseReply, error) {
		return &schema.UseDatabaseReply{Token: "token"}, nil
	}
	setFOK := func(ctx context.Context, key []byte, value []byte) (*schema.Index, error) {
		data[string(key)] = string(value)
		r := schema.Index{Index: index}
		index++
		return &r, nil
	}
	icm := &clienttest.ImmuClientMock{
		GetOptionsF: func() *client.Options {
			return client.DefaultOptions()
		},
		LoginF:       loginFOK,
		DisconnectF:  disconnectFOK,
		UseDatabaseF: useDatabaseFOK,
		SetF:         setFOK,
	}

	pwrMockOK := &pwrMock{
		readF: func(string) ([]byte, error) { return []byte("password"), nil },
	}

	trMockOK := &trMock{
		ReadFromTerminalYNF: func(def string) (selected string, err error) {
			return "Y", nil
		},
	}

	newClient := func(opts *client.Options) (client.ImmuClient, error) {
		return icm, nil
	}

	hds := &homedirServiceMock{
		WriteFileToUserHomeDirF: func(content []byte, pathToFile string) error {
			return nil
		},
		FileExistsInUserHomeDirF: func(pathToFile string) (bool, error) {
			return false, nil
		},
		ReadFileFromUserHomeDirF: func(pathToFile string) (string, error) {
			return "", nil
		},
		DeleteFileFromUserHomeDirF: func(pathToFile string) error {
			return nil
		},
	}

	errFunc := func(err error) {
		require.NoError(t, err)
	}

	cmd1 := NewCmd(newClient, pwrMockOK, trMockOK, hds, errFunc)
	cmd1.SetArgs([]string{"3"})
	cmd1.Execute()
	require.Equal(t, 3, len(data))

	hds2 := *hds
	hdsWriteErr := errors.New("hds write error")
	hds2.WriteFileToUserHomeDirF = func(content []byte, pathToFile string) error {
		return hdsWriteErr
	}
	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, hdsWriteErr, err)
	}
	cmd2 := NewCmd(newClient, pwrMockOK, trMockOK, &hds2, errFunc)
	cmd2.SetArgs([]string{"3"})
	cmd2.Execute()

	viper.Set("user", "someuser")
	cmd3 := NewCmd(newClient, pwrMockOK, trMockOK, hds, errFunc)
	cmd3.SetArgs([]string{"3"})
	cmd3.Execute()

	icErr := errors.New("some immuclient error")
	newClientErrFunc := func(opts *client.Options) (client.ImmuClient, error) {
		return nil, icErr
	}
	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, icErr, err)
	}
	cmd4 := NewCmd(newClientErrFunc, pwrMockOK, trMockOK, hds, errFunc)
	cmd4.SetArgs([]string{"3"})
	cmd4.Execute()

	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, `strconv.Atoi: parsing "a": invalid syntax`, err.Error())
	}
	cmd5 := NewCmd(newClient, pwrMockOK, trMockOK, hds, errFunc)
	cmd5.SetArgs([]string{"a"})
	cmd5.Execute()

	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(
			t,
			`Please specify a number of entries greater than 0 or call the command without any argument so that the default number of 100 entries will be used`,
			err.Error())
	}
	cmd6 := NewCmd(newClient, pwrMockOK, trMockOK, hds, errFunc)
	cmd6.SetArgs([]string{"0"})
	cmd6.Execute()

	pwrErr := errors.New("pwr read error")
	pwrErrMock := &pwrMock{
		readF: func(string) ([]byte, error) { return nil, pwrErr },
	}
	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, pwrErr, err)
	}
	cmd7 := NewCmd(newClient, pwrErrMock, trMockOK, hds, errFunc)
	cmd7.SetArgs([]string{"1"})
	cmd7.Execute()

	loginErr := errors.New("some login err")
	icm.LoginF = func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		return nil, loginErr
	}
	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, loginErr, err)
	}
	cmd8 := NewCmd(newClient, pwrMockOK, trMockOK, hds, errFunc)
	cmd8.SetArgs([]string{"1"})
	cmd8.Execute()

	icm.LoginF = loginFOK
	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, hdsWriteErr, err)
	}
	cmd9 := NewCmd(newClient, pwrMockOK, trMockOK, &hds2, errFunc)
	cmd9.SetArgs([]string{"1"})
	cmd9.Execute()

	errUseDb := errors.New("some use db error")
	icm.UseDatabaseF = func(ctx context.Context, d *schema.Database) (*schema.UseDatabaseReply, error) {
		return nil, errUseDb
	}
	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, errUseDb, err)
	}
	cmd10 := NewCmd(newClient, pwrMockOK, trMockOK, hds, errFunc)
	cmd10.SetArgs([]string{"1"})
	cmd10.Execute()

	icm.UseDatabaseF = useDatabaseFOK
	trErrMock := &trMock{
		ReadFromTerminalYNF: func(def string) (selected string, err error) {
			return "", errors.New("some tr error")
		},
	}
	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, "Canceled", err.Error())
	}
	cmd11 := NewCmd(newClient, pwrMockOK, trErrMock, hds, errFunc)
	cmd11.SetArgs([]string{"1"})
	cmd11.Execute()

	errSet := errors.New("some set error")
	icm.SetF = func(ctx context.Context, key []byte, value []byte) (*schema.Index, error) {
		return nil, errSet
	}
	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, errSet, err)
	}
	cmd12 := NewCmd(newClient, pwrMockOK, trMockOK, hds, errFunc)
	cmd12.SetArgs([]string{"1"})
	cmd12.Execute()

	icm.SetF = setFOK
	errDisconnect := errors.New("some disconnect error")
	icm.DisconnectF = func() error { return errDisconnect }
	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, errDisconnect, err)
	}
	cmd13 := NewCmd(newClient, pwrMockOK, trMockOK, hds, errFunc)
	cmd13.SetArgs([]string{"1"})
	cmd13.Execute()
}
