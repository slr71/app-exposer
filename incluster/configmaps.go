package incluster

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cyverse-de/model/v8"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// excludesConfigMapName returns the name of the ConfigMap containing the list
// of paths that should be excluded from file uploads to iRODS by porklock.
func excludesConfigMapName(job *model.Job) string {
	return fmt.Sprintf("excludes-file-%s", job.InvocationID)
}

// excludesFileContents returns a *bytes.Buffer containing the contents of an
// file exclusion list that gets passed to porklock to prevent it from uploading
// content. It's possible that the buffer is empty, but it shouldn't be nil.
func excludesFileContents(job *model.Job) *bytes.Buffer {
	var output bytes.Buffer

	for _, p := range job.ExcludeArguments() {
		output.WriteString(fmt.Sprintf("%s\n", p))
	}
	return &output
}

// excludesConfigMap returns the ConfigMap containing the list of paths
// that should be excluded from file uploads to iRODS by porklock. This does NOT
// call the k8s API to actually create the ConfigMap, just returns the object
// that can be passed to the API.
func (i *Incluster) excludesConfigMap(ctx context.Context, job *model.Job) (*apiv1.ConfigMap, error) {
	labels, err := i.labelsFromJob(ctx, job)
	if err != nil {
		return nil, err
	}

	return &apiv1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:   excludesConfigMapName(job),
			Labels: labels,
		},
		Data: map[string]string{
			excludesFileName: excludesFileContents(job).String(),
		},
	}, nil
}

// inputPathListConfigMapName returns the name of the ConfigMap containing
// the list of paths that should be downloaded from iRODS by porklock
// as input files for the VICE analysis.
func inputPathListConfigMapName(job *model.Job) string {
	return fmt.Sprintf("input-path-list-%s", job.InvocationID)
}

// inputPathListContents returns a *bytes.Buffer containing the contents of a
// input path list file. Does not write out the contents to a file. Returns
// (nil, nil) if there aren't any inputs without tickets associated with the
// Job.
func inputPathListContents(job *model.Job, pathListIdentifier string) (*bytes.Buffer, error) {
	buffer := bytes.NewBufferString("")

	// Add the path list identifier.
	_, err := fmt.Fprintf(buffer, "%s\n", pathListIdentifier)
	if err != nil {
		return nil, err
	}

	// Add the list of paths.
	for _, input := range job.FilterInputsWithoutTickets() {
		_, err = fmt.Fprintf(buffer, "%s\n", input.IRODSPath())
		if err != nil {
			return nil, err
		}
	}

	return buffer, nil
}

// inputPathListConfigMap returns the ConfigMap object containing the the
// list of paths that should be downloaded from iRODS by porklock as input
// files for the VICE analysis. This does NOT call the k8s API to actually
// create the ConfigMap, just returns the object that can be passed to the API.
func (i *Incluster) inputPathListConfigMap(ctx context.Context, job *model.Job) (*apiv1.ConfigMap, error) {
	labels, err := i.labelsFromJob(ctx, job)
	if err != nil {
		return nil, err
	}

	fileContents, err := inputPathListContents(job, i.InputPathListIdentifier)
	if err != nil {
		return nil, err
	}

	return &apiv1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:   inputPathListConfigMapName(job),
			Labels: labels,
		},
		Data: map[string]string{
			inputPathListFileName: fileContents.String(),
		},
	}, nil
}
