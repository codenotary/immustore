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

package main

import (
	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/cmd/immuclient/commands"

	"github.com/spf13/cobra"
)

var App = "immuclient"
var Version string
var Commit string
var BuiltBy string
var BuiltAt string

var o = &c.Options{}

func init() {
	cobra.OnInitialize(func() { o.InitConfig("immuclient") })
}

func main() {
	cmd := &cobra.Command{
		Use:   "immuclient",
		Short: "CLI client for immudb - the lightweight, high-speed immutable database for systems and applications",
		Long: `CLI client for immudb - the lightweight, high-speed immutable database for systems and applications.
Environment variables:
  IMMUCLIENT_IMMUDB-ADDRESS=127.0.0.1
  IMMUCLIENT_IMMUDB-PORT=3322
  IMMUCLIENT_AUTH=false
  IMMUCLIENT_MTLS=false
  IMMUCLIENT_SERVERNAME=localhost
  IMMUCLIENT_PKEY=./tools/mtls/4_client/private/localhost.key.pem
  IMMUCLIENT_CERTIFICATE=./tools/mtls/4_client/certs/localhost.cert.pem
  IMMUCLIENT_CLIENTCAS=./tools/mtls/2_intermediate/certs/ca-chain.cert.pem`,
		DisableAutoGenTag: true,
	}

	commands.Init(cmd, o)
	cmd.AddCommand(c.VersionCmd(App, Version, Commit, BuiltBy, BuiltAt))

	if err := cmd.Execute(); err != nil {
		c.QuitToStdErr(err)
	}
}
