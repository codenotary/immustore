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

package audit

import (
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func TestInitAgent(t *testing.T) {
	srvoptions := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(srvoptions)
	bs.Start()

	os.Setenv("audit-agent-interval", "1s")
	pidPath := "pid_path"
	viper.Set("pidfile", pidPath)
	ad := new(auditAgent)

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	ad.opts = options().WithMetrics(false).WithDialOptions(&dialOptions).WithMTLs(false)
	_, err := ad.InitAgent()
	if err != nil {
		t.Fatal("InitAgent", err)
	}
	os.RemoveAll(pidPath)
}
