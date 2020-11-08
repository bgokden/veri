// MIT License
// Copyright (c) 2018 Berk Gokden
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package cmd

import (
	veriserviceserver "github.com/bgokden/veri/server"
	"github.com/spf13/cobra"
)

var services string
var port int
var broadcastAdresses string
var directory string

// var tls bool
// var cert string
// var key string

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "serve veri",
	Long: `Serve veri service:
  veri serve
  `,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		configMap := make(map[string]interface{})
		configMap["services"] = services
		configMap["port"] = port
		// configMap["tls"] = tls
		// configMap["cert"] = cert
		// configMap["key"] = key
		configMap["broadcast"] = broadcastAdresses
		configMap["directory"] = directory
		veriserviceserver.RunServer(configMap)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)
	serveCmd.Flags().StringVarP(&services, "services", "s", "", "services to connect, Comma separated lists are supported")
	serveCmd.Flags().IntVarP(&port, "port", "p", 10000, "port")
	serveCmd.Flags().StringVarP(&broadcastAdresses, "broadcast", "b", "", "broadcast adresses to advertise, Comma separated lists are supported")
	serveCmd.Flags().StringVarP(&directory, "directory", "d", "", "data directory")

	// serveCmd.Flags().BoolVarP(&tls, "tls", "t", false, "enable tls")
	// serveCmd.Flags().StringVarP(&cert, "cert", "", "", "cert file path")
	// serveCmd.Flags().StringVarP(&key, "key", "", "", "key file path")

	//TODO: serveCmd.Flags().StringSliceVarP(&services, "services", "", []string{}, "Services to connect, Comma separated lists are supported")
}
