package cmd

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	semver "github.com/bgokden/veri/semver"
)

var cfgFile string
var Verbose bool

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

	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.veri/config.yaml")
	rootCmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "Verbose output")

	viper.BindEnv("config", "VERICONFIG")

}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.AutomaticEnv() // read in environment variables that match
	if cfgFile == "" {
		cfgFile = viper.GetString("config")
	}
	log.Printf("Using Config file path: %v\n", cfgFile)

	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, homeDirError := homedir.Dir()
		if homeDirError != nil {
			log.Printf("Can not find home Directory: %v\n", homeDirError)
			os.Exit(1)
		}
		// Search config in home directory with name ".veri" (without extension).
		path := filepath.FromSlash(home + "/.veri")
		viper.AddConfigPath(path)
		viper.SetConfigName("config")
	}
	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Printf("Using config file: %v\n", viper.ConfigFileUsed())
	} else {
		log.Printf("Config can not be read due to error: %v\n", err)
	}
}
