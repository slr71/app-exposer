package constants

const (
	AnalysisContainerName = "analysis"

	PorklockConfigVolumeName = "porklock-config"
	PorklockConfigSecretName = "porklock-config"
	PorklockConfigMountPath  = "/etc/porklock"

	CSIDriverName                      = "irods.csi.cyverse.org"
	CSIDriverStorageClassName          = "irods-sc"
	CSIDriverDataVolumeNamePrefix      = "csi-data-volume"
	CSIDriverDataVolumeClaimNamePrefix = "csi-data-volume-claim"
	CSIDriverInputVolumeMountPath      = "/input"
	CSIDriverOutputVolumeMountPath     = "/output"
	CSIDriverLocalMountPath            = "/data-store"

	// The file transfers volume serves as the working directory when IRODS CSI Driver integration is disabled.
	FileTransfersContainerName     = "input-files"
	FileTransfersInitContainerName = "input-files-init"
	FileTransfersInputsMountPath   = "/input-files"

	// Constants for shared memory volumes.
	SharedMemoryVolumeName = "shared-memory"

	// The working directory volume serves as the working directory when IRODS CSI Driver integration is enabled.
	WorkingDirVolumeName             = "working-dir"
	WorkingDirInitContainerName      = "working-dir-init"
	WorkingDirInitContainerMountPath = "/working-dir"

	// The persistent data volume name. The volume is used to persist data locally across restarts.
	LocalDirVolumeName = "analysis-data"

	VICEProxyContainerName = "vice-proxy"
	VICEProxyPort          = int32(60002)
	VICEProxyPortName      = "tcp-proxy"
	VICEProxyServicePort   = int32(60000)

	ExcludesMountPath  = "/excludes"
	ExcludesFileName   = "excludes-file"
	ExcludesVolumeName = "excludes-file"

	PermissionsConfigMapPrefix = "permissions"
	PermissionsVolumeName      = "vice-permissions"
	PermissionsMountPath       = "/etc/vice-permissions"
	PermissionsFileName        = "allowed-users"

	// The CA bundle vice-proxy trusts when the DE's certificates chain to a
	// private CA. Matches the mount path the DE services use.
	CABundleVolumeName = "de-ca"
	CABundleMountPath  = "/etc/de-ca"
	CABundleKey        = "ca.crt"

	InputPathListMountPath  = "/input-paths"
	InputPathListFileName   = "input-path-list"
	InputPathListVolumeName = "input-path-list"

	IRODSConfigFilePath = "/etc/porklock/irods-config.properties"

	FileTransfersPortName = "tcp-input"
	FileTransfersPort     = int32(60001)

	DownloadBasePath = "/download"
	UploadBasePath   = "/upload"
	DownloadKind     = "download"
	UploadKind       = "upload"

	GPUTolerationKey      = "gpu"
	GPUTolerationOperator = "Equal"
	GPUTolerationValue    = "true"
	GPUTolerationEffect   = "NoSchedule"

	AnalysisAffinityKey      = "analysis"
	AnalysisAffinityOperator = "Exists"

	GPUAffinityKey           = "gpu"
	GPUAffinityOperator      = "In"
	GPUAffinityValue         = "true"
	GPUModelAffinityKey      = "nvidia.com/gpu.product"
	GPUModelAffinityOperator = "In"

	DefaultUserSuffix = "@iplantcollaborative.org"

	ShmDevice = "/dev/shm"

	// Labels that identify VICE resources across the cluster. These
	// travel on Deployments, Services, HTTPRoutes, ConfigMaps, and
	// PVCs; every listing/deletion/reconciliation path does label-
	// based matching against them. A typo at any one site silently
	// breaks matching, so the keys live here rather than as raw
	// string literals in each package.
	AnalysisIDLabel   = "analysis-id"
	AnalysisNameLabel = "analysis-name"
	AppNameLabel      = "app-name"
	AppTypeLabel      = "app-type"
	AppIDLabel        = "app-id"
	ExternalIDLabel   = "external-id"
	UsernameLabel     = "username"
	UserIDLabel       = "user-id"
	SubdomainLabel    = "subdomain"
	LoginIPLabel      = "login-ip"
)

// AnalysisID is the app-exposer/DE analysis identifier — the "id" column
// on the jobs table. The apps service always assigns an AnalysisID to
// the analysis as a whole, whether it's a single-step job or a pipeline
// that spans multiple execution systems.
//
// ExternalID is the per-step job identifier within a single execution
// system. The apps service always provides one as well: for systems
// that assign their own job IDs (e.g. TAPIS) it records that system's
// ID; for app-exposer, which has no native job ID, apps synthesizes
// one. ExternalID appears on job_status_updates and on pod labels, and
// is what callers look up when they hold a single step rather than a
// whole analysis.
//
// Both are stored and transmitted as strings; the domain types exist so
// the compiler catches accidental swaps and so function signatures
// advertise which identifier they expect. Both are transparent to sqlx,
// encoding/json, and fmt because their underlying kind is string.
type (
	AnalysisID string
	ExternalID string
)

type AnalysisStatus string

const (
	Running   AnalysisStatus = "Running"
	Completed AnalysisStatus = "Completed"
	Failed    AnalysisStatus = "Failed"
)

// AnalysisKind corresponds to the job types in the database. We've started using the term
// "analysis" instead of job and type is a reserved word in Go.
type AnalysisKind string

const (
	// Interactive analyses are VICE analyses.
	Interactive AnalysisKind = "interactive"

	// Internal analyses are tools like URL import that aren't accessible directly by the user.
	Internal AnalysisKind = "internal"

	// Executable analyses are batch analyses.
	Executable AnalysisKind = "executable"
)

// Int32Ptr returns a pointer to the given int32 value. Useful when a K8s
// struct field requires a *int32 (e.g. Deployment.Spec.Replicas).
func Int32Ptr(i int32) *int32 { return &i }

// Int64Ptr returns a pointer to the given int64 value. Useful when a K8s
// struct field requires a *int64 (e.g. SecurityContext.RunAsUser).
func Int64Ptr(i int64) *int64 { return &i }

// StringPtr returns a pointer to the given string value. Useful when a K8s
// struct field requires a *string (e.g. PodDNSConfigOption.Value).
func StringPtr(s string) *string { return &s }
