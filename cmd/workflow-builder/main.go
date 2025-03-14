package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/cyverse-de/app-exposer/batch"
	"github.com/cyverse-de/app-exposer/imageinfo"
	"github.com/cyverse-de/model/v8"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var (
		err        error
		kubeconfig *string
		inputJob   model.Job

		job                = flag.String("job", "", "The file containing the job definition. Required.")
		transferImage      = flag.String("transfer-image", "harbor.cyverse.org/de/gocmd:latest", "(optional) Image used to transfer files to/from the data store")
		transferWorkingDir = flag.String("transfer-working-dir", "/de-app-work", "The working directory within the file transfer image.")
		transferLogLevel   = flag.String("transfer-log-level", "debug", "The log level of the output of the file transfer tool.")
		statusSenderImage  = flag.String("status-sender-image", "harbor.cyverse.org/de/url-import:latest", "The image used to send status updates. Must container curl.")
		analysisID         = flag.String("analysis-id", "", "The unique identifier for the analysis.")
		quiet              = flag.Bool("quiet", false, "Whether to turn off printing out the workflow.")
		doSubmit           = flag.Bool("submit", false, "Whether to submit the workflow to the cluster.")
		out                = flag.String("out", "", "The file the workflow will be written to.")
		harborURL          = flag.String("harbor-url", "https://harbor.cyverse.org/api/v2.0/", "The base URL for the harbor instance")
		harborUser         = flag.String("harbor-user", "", "The user for harbor lookups.")
		harborPass         = flag.String("harbor-pass", "", "The password for the harbor user.")
	)

	// Prefer the value in the KUBECONFIG env var.
	// If the value is not set, then check for the HOME directory.
	// If that is not set, then require the user to specify a path.
	if kubeconfigEnv := os.Getenv("KUBECONFIG"); kubeconfigEnv != "" {
		kubeconfig = flag.String("kubeconfig", kubeconfigEnv, "absolute path to the kubeconfig file")
	} else if home := os.Getenv("HOME"); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		// If the home directory doesn't exist, then allow the user to specify a path.
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}

	flag.Parse()

	if *job == "" {
		log.Fatal("--job must be set.")
	}

	if *analysisID == "" {
		log.Fatal("--analysis-id must be set.")
	}

	if *harborUser == "" {
		log.Fatal("--harbor-user must be set.")
	}

	if *harborPass == "" {
		log.Fatal("--harbor-pass must be set")
	}

	var config *rest.Config
	if *kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			log.Fatal(errors.Wrapf(err, "error building config from flags using kubeconfig %s", *kubeconfig))
		}
	} else {
		// If the home directory doesn't exist and the user doesn't specify a path,
		// then assume that we're running inside a cluster.
		config, err = rest.InClusterConfig()
		if err != nil {
			log.Fatal(errors.Wrapf(err, "error loading the config inside the cluster"))
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(errors.Wrap(err, "error creating clientset from config"))
	}

	infoGetter, err := imageinfo.NewHarborInfoGetter(*harborURL, *harborUser, *harborPass)
	if err != nil {
		log.Fatal(err)
	}

	infile, err := os.Open(*job)
	if err != nil {
		log.Fatal(err)
	}
	defer infile.Close()

	if err = json.NewDecoder(infile).Decode(&inputJob); err != nil {
		log.Fatal(err)
	}

	opts := batch.BatchSubmissionOpts{
		FileTransferImage:      *transferImage,
		FileTransferWorkingDir: *transferWorkingDir,
		FileTransferLogLevel:   *transferLogLevel,
		StatusSenderImage:      *statusSenderImage,
		ExternalID:             *analysisID,
	}

	maker := batch.NewWorkflowMaker(infoGetter, &inputJob, clientset)
	workflow, err := maker.NewWorkflow(context.Background(), &opts)
	if err != nil {
		log.Fatal(err)
	}

	if !*quiet {
		var outfile *os.File

		if *out == "" {
			outfile = os.Stdout
		} else {
			outfile, err := os.Create(*out)
			if err != nil {
				log.Fatal(err)
			}
			defer outfile.Close()
		}

		if err = yaml.NewEncoder(outfile).Encode(&workflow); err != nil {
			log.Fatal(err)
		}
	}

	if *doSubmit {
		ctx, cl, err := batch.NewWorkflowServiceClient(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		if _, err = batch.SubmitWorkflow(ctx, cl, workflow); err != nil {
			log.Fatal(err)
		}
	}
}
