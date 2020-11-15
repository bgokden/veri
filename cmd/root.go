package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/magneticio/go-common/logging"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	semver "github.com/magneticio/vampkubistcli/semver"
)

var cfgFile string

// Version should be in format vd.d.d where d is a decimal number
const Version string = semver.Version

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "veri",
	Short: "A command line client for veri",
	Long: `A command line client for veri:
  veri serve
  `,
	SilenceUsage:  true,
	SilenceErrors: true,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {

	logging.Init(os.Stdout, os.Stderr)

	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.veri/config.yaml")
	rootCmd.PersistentFlags().BoolVarP(&logging.Verbose, "verbose", "v", false, "Verbose output")

	viper.BindEnv("config", "VERICONFIG")

}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.AutomaticEnv() // read in environment variables that match
	if cfgFile == "" {
		cfgFile = viper.GetString("config")
	}
	logging.Info("Using Config file path: %v\n", cfgFile)

	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, homeDirError := homedir.Dir()
		if homeDirError != nil {
			logging.Error("Can not find home Directory: %v\n", homeDirError)
			os.Exit(1)
		}
		// Search config in home directory with name ".veri" (without extension).
		path := filepath.FromSlash(home + "/.veri")
		viper.AddConfigPath(path)
		viper.SetConfigName("config")
	}
	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		logging.Info("Using config file: %v\n", viper.ConfigFileUsed())
	} else {
		logging.Error("Config can not be read due to error: %v\n", err)
	}
}
