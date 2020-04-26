package commands

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/pkg/api"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/spf13/cobra"
)

func (cl *commandline) rawSafeSetKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "rawsafeset key",
		Short:   "Set item having the specified key, without setup structured values",
		Aliases: []string{"rs"},
		RunE: func(cmd *cobra.Command, args []string) error {

			key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			val, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			immuClient := cl.getImmuClient(cmd)
			rootService := client.NewRootService(immuClient.ServiceClient, cache.NewFileCache())
			root, err := rootService.GetRoot(ctx)
			if err != nil {
				c.QuitToStdErr(err)
			}

			opts := &schema.SafeSetOptions{
				Kv: &schema.KeyValue{
					Key:   key,
					Value: val,
				},
				RootIndex: &schema.Index{
					Index: root.Index,
				},
			}

			proof, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.ServiceClient.SafeSet(ctx, opts)
			})
			if err != nil {
				c.QuitWithUserError(err)
			}
			p := proof.(*schema.Proof)
			leaf := api.Digest(p.Index, key, val)
			verified := p.Verify(leaf[:], *root)

			if err != nil {
				c.QuitWithUserError(err)
			}
			if verified {
				tocache := new(schema.Root)
				tocache.Index = p.Index
				tocache.Root = p.Root
				err = rootService.SetRoot(tocache)
				if err != nil {
					c.QuitWithUserError(err)
				}
			}

			vi := &client.VerifiedItem{
				Key:      key,
				Value:    val,
				Index:    p.At,
				Verified: verified,
			}
			printItem(vi.Key, vi.Value, vi)
			return nil
		},
		Args: cobra.ExactArgs(2),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) setKeyValue(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "set key value",
		Short:   "Add new item having the specified key and value",
		Aliases: []string{"s"},
		RunE: func(cmd *cobra.Command, args []string) error {

			var reader io.Reader
			if len(args) > 1 {
				reader = bytes.NewReader([]byte(args[1]))
			} else {
				reader = bufio.NewReader(os.Stdin)
			}
			var buf bytes.Buffer
			tee := io.TeeReader(reader, &buf)
			key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			value, err := ioutil.ReadAll(tee)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			immuClient := cl.getImmuClient(cmd)
			_, err = immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.Set(ctx, key, value)
			})
			if err != nil {
				c.QuitWithUserError(err)
			}
			value2, err := ioutil.ReadAll(&buf)
			if err != nil {
				c.QuitToStdErr(err)
			}
			response, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.Get(ctx, key)
			})
			if err != nil {
				c.QuitToStdErr(err)
			}
			printItem([]byte(args[0]), value2, response)
			return nil
		},
		Args: cobra.ExactArgs(2),
	}

	cmd.AddCommand(ccmd)
}

func (cl *commandline) safeSetKeyValue(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "safeset key value",
		Short:   "Add and verify new item having the specified key and value",
		Aliases: []string{"ss"},
		RunE: func(cmd *cobra.Command, args []string) error {

			var reader io.Reader
			if len(args) > 1 {
				reader = bytes.NewReader([]byte(args[1]))
			} else {
				reader = bufio.NewReader(os.Stdin)
			}
			key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			var buf bytes.Buffer
			tee := io.TeeReader(reader, &buf)
			value, err := ioutil.ReadAll(tee)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			immuClient := cl.getImmuClient(cmd)
			_, err = immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.SafeSet(ctx, key, value)
			})
			if err != nil {
				c.QuitWithUserError(err)
			}
			value2, err := ioutil.ReadAll(&buf)
			if err != nil {
				c.QuitToStdErr(err)
			}
			response, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.SafeGet(ctx, key)
			})
			if err != nil {
				c.QuitToStdErr(err)
			}
			printItem([]byte(args[0]), value2, response)
			return nil
		},
		Args: cobra.ExactArgs(2),
	}
	cmd.AddCommand(ccmd)
}
func (cl *commandline) zAddSetNameScoreKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "zadd setname score key",
		Short:   "Add new key with score to a new or existing sorted set",
		Aliases: []string{"za"},
		RunE: func(cmd *cobra.Command, args []string) error {

			var setReader io.Reader
			var scoreReader io.Reader
			var keyReader io.Reader
			if len(args) > 1 {
				setReader = bytes.NewReader([]byte(args[0]))
				scoreReader = bytes.NewReader([]byte(args[1]))
				keyReader = bytes.NewReader([]byte(args[2]))
			}

			bs, err := ioutil.ReadAll(scoreReader)
			score, err := strconv.ParseFloat(string(bs[:]), 64)
			if err != nil {
				c.QuitToStdErr(err)
			}
			set, err := ioutil.ReadAll(setReader)
			if err != nil {
				c.QuitToStdErr(err)
			}
			key, err := ioutil.ReadAll(keyReader)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			immuClient := cl.getImmuClient(cmd)
			response, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.ZAdd(ctx, set, score, key)
			})
			if err != nil {
				c.QuitWithUserError(err)
			}
			printSetItem([]byte(args[0]), []byte(args[2]), score, response)
			return nil
		},
		Args: cobra.MinimumNArgs(3),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) safeZAddSetNameScoreKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "safezadd setname score key",
		Short:   "Add and verify new key with score to a new or existing sorted set",
		Aliases: []string{"sza"},
		RunE: func(cmd *cobra.Command, args []string) error {

			var setReader io.Reader
			var scoreReader io.Reader
			var keyReader io.Reader
			if len(args) > 1 {
				setReader = bytes.NewReader([]byte(args[0]))
				scoreReader = bytes.NewReader([]byte(args[1]))
				keyReader = bytes.NewReader([]byte(args[2]))
			}

			bs, err := ioutil.ReadAll(scoreReader)
			if err != nil {
				c.QuitToStdErr(err)
			}
			score, err := strconv.ParseFloat(string(bs[:]), 64)
			if err != nil {
				c.QuitToStdErr(err)
			}
			set, err := ioutil.ReadAll(setReader)
			if err != nil {
				c.QuitToStdErr(err)
			}
			key, err := ioutil.ReadAll(keyReader)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			immuClient := cl.getImmuClient(cmd)
			response, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.SafeZAdd(ctx, set, score, key)
			})
			if err != nil {
				c.QuitWithUserError(err)
			}
			printSetItem([]byte(args[0]), []byte(args[2]), score, response)
			return nil
		},
		Args: cobra.MinimumNArgs(3),
	}
	cmd.AddCommand(ccmd)
}
