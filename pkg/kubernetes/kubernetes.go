package kubernetes

import (
	"context"
	"errors"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/fsnotify/fsnotify"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/klog/v2"

	"github.com/containers/kubernetes-mcp-server/pkg/config"
	"github.com/containers/kubernetes-mcp-server/pkg/helm"

	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
)

type HeaderKey string

const (
	CustomAuthorizationHeader = HeaderKey("kubernetes-authorization")
	OAuthAuthorizationHeader  = HeaderKey("Authorization")

	CustomUserAgent = "kubernetes-mcp-server/bearer-token-auth"
)

type CloseWatchKubeConfig func() error

type Kubernetes struct {
	manager *Manager
}

// ContextManager manages multiple Kubernetes contexts and their associated managers
type ContextManager struct {
	staticConfig    *config.StaticConfig
	contextManagers map[string]*Manager
	defaultManager  *Manager
	mutex           sync.RWMutex
}

// NewContextManager creates a new context manager
func NewContextManager(staticConfig *config.StaticConfig) (*ContextManager, error) {
	defaultManager, err := NewManager(staticConfig)
	if err != nil {
		return nil, err
	}

	cm := &ContextManager{
		staticConfig:    staticConfig,
		contextManagers: make(map[string]*Manager),
		defaultManager:  defaultManager,
	}

	// Setup kubeconfig watching for the default manager
	defaultManager.WatchKubeConfig(cm.reloadKubernetesClients)

	return cm, nil
}

// GetManagerForContext returns a manager for the specified context, creating it if necessary
func (cm *ContextManager) GetManagerForContext(contextName string) (*Manager, error) {
	if contextName == "" {
		return cm.defaultManager, nil
	}

	cm.mutex.RLock()
	if manager, exists := cm.contextManagers[contextName]; exists {
		cm.mutex.RUnlock()
		return manager, nil
	}
	cm.mutex.RUnlock()

	// Create new manager for the context
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	// Double-check after acquiring write lock
	if manager, exists := cm.contextManagers[contextName]; exists {
		return manager, nil
	}

	// Create a new config with the specified context
	configWithContext := *cm.staticConfig
	configWithContext.KubeContext = contextName

	manager, err := NewManager(&configWithContext)
	if err != nil {
		return nil, err
	}

	cm.contextManagers[contextName] = manager
	klog.V(3).Infof("Created new Kubernetes manager for context: %s", contextName)

	return manager, nil
}

// Derived returns a derived Kubernetes client for the default context
func (cm *ContextManager) Derived(ctx context.Context) (*Kubernetes, error) {
	return cm.GetDefaultManager().Derived(ctx)
}

// Derived returns a derived Kubernetes client for the specified context
func (cm *ContextManager) DerivedContext(ctx context.Context, kubeContext string) (*Kubernetes, error) {
	manager, err := cm.GetManagerForContext(kubeContext)
	if err != nil {
		return nil, err
	}
	return manager.Derived(ctx)
}

// DerivedFromRequest returns a derived Kubernetes client based on context from CallToolRequest
func (cm *ContextManager) DerivedFromRequest(ctx context.Context, ctr interface{}) (*Kubernetes, error) {
	var kubeContext string

	// Try to extract context from arguments if available
	if callRequest, ok := ctr.(interface{ GetArguments() map[string]interface{} }); ok {
		if contextArg := callRequest.GetArguments()["context"]; contextArg != nil {
			if contextStr, ok := contextArg.(string); ok {
				kubeContext = contextStr
			}
		}
	}

	return cm.DerivedContext(ctx, kubeContext)
}

// reloadKubernetesClients reloads all kubernetes clients when kubeconfig changes
func (cm *ContextManager) reloadKubernetesClients() error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	// Reload default manager
	if err := cm.defaultManager.initKubernetesClient(); err != nil {
		klog.Errorf("Failed to reload default kubernetes client: %v", err)
	}

	// Clear context managers cache to force recreation on next access
	for contextName, manager := range cm.contextManagers {
		manager.Close()
		delete(cm.contextManagers, contextName)
		klog.V(3).Infof("Cleared cached manager for context: %s", contextName)
	}

	return nil
}

// Close closes all managers and stops watching
func (cm *ContextManager) Close() {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if cm.defaultManager != nil {
		cm.defaultManager.Close()
	}

	for _, manager := range cm.contextManagers {
		manager.Close()
	}
}

func (cm *ContextManager) GetDefaultManager() *Manager {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.defaultManager
}

type Manager struct {
	cfg                     *rest.Config
	clientCmdConfig         clientcmd.ClientConfig
	discoveryClient         discovery.CachedDiscoveryInterface
	accessControlClientSet  *AccessControlClientset
	accessControlRESTMapper *AccessControlRESTMapper
	dynamicClient           *dynamic.DynamicClient

	staticConfig         *config.StaticConfig
	CloseWatchKubeConfig CloseWatchKubeConfig
}

var Scheme = scheme.Scheme
var ParameterCodec = runtime.NewParameterCodec(Scheme)

var _ helm.Kubernetes = &Manager{}

func NewManager(config *config.StaticConfig) (*Manager, error) {
	m := &Manager{
		staticConfig: config,
	}
	if err := m.initKubernetesClient(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) initKubernetesClient() error {
	if err := resolveKubernetesConfigurations(m); err != nil {
		return err
	}

	var err error
	m.accessControlClientSet, err = NewAccessControlClientset(m.cfg, m.staticConfig)
	if err != nil {
		return err
	}
	m.discoveryClient = memory.NewMemCacheClient(m.accessControlClientSet.DiscoveryClient())
	m.accessControlRESTMapper = NewAccessControlRESTMapper(
		restmapper.NewDeferredDiscoveryRESTMapper(m.discoveryClient),
		m.staticConfig,
	)
	m.dynamicClient, err = dynamic.NewForConfig(m.cfg)
	if err != nil {
		return err
	}
	return nil
}

func (m *Manager) WatchKubeConfig(onKubeConfigChange func() error) {
	if m.clientCmdConfig == nil {
		return
	}
	kubeConfigFiles := m.clientCmdConfig.ConfigAccess().GetLoadingPrecedence()
	if len(kubeConfigFiles) == 0 {
		return
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return
	}
	for _, file := range kubeConfigFiles {
		_ = watcher.Add(file)
	}
	go func() {
		for {
			select {
			case _, ok := <-watcher.Events:
				if !ok {
					return
				}
				_ = onKubeConfigChange()
			case _, ok := <-watcher.Errors:
				if !ok {
					return
				}
			}
		}
	}()
	m.Close() // Close any previous watcher
	m.CloseWatchKubeConfig = watcher.Close
}

func (m *Manager) Close() {
	if m.CloseWatchKubeConfig != nil {
		_ = m.CloseWatchKubeConfig()
	}
}

func (m *Manager) GetAPIServerHost() string {
	if m.cfg == nil {
		return ""
	}
	return m.cfg.Host
}

func (m *Manager) ToDiscoveryClient() (discovery.CachedDiscoveryInterface, error) {
	return m.discoveryClient, nil
}

func (m *Manager) ToRESTMapper() (meta.RESTMapper, error) {
	return m.accessControlRESTMapper, nil
}

func (m *Manager) Derived(ctx context.Context) (*Kubernetes, error) {
	authorization, ok := ctx.Value(OAuthAuthorizationHeader).(string)
	if !ok || !strings.HasPrefix(authorization, "Bearer ") {
		if m.staticConfig.RequireOAuth {
			return nil, errors.New("oauth token required")
		}
		return &Kubernetes{manager: m}, nil
	}
	klog.V(5).Infof("%s header found (Bearer), using provided bearer token", OAuthAuthorizationHeader)
	derivedCfg := &rest.Config{
		Host:    m.cfg.Host,
		APIPath: m.cfg.APIPath,
		// Copy only server verification TLS settings (CA bundle and server name)
		TLSClientConfig: rest.TLSClientConfig{
			Insecure:   m.cfg.Insecure,
			ServerName: m.cfg.ServerName,
			CAFile:     m.cfg.CAFile,
			CAData:     m.cfg.CAData,
		},
		BearerToken: strings.TrimPrefix(authorization, "Bearer "),
		// pass custom UserAgent to identify the client
		UserAgent:   CustomUserAgent,
		QPS:         m.cfg.QPS,
		Burst:       m.cfg.Burst,
		Timeout:     m.cfg.Timeout,
		Impersonate: rest.ImpersonationConfig{},
	}
	clientCmdApiConfig, err := m.clientCmdConfig.RawConfig()
	if err != nil {
		if m.staticConfig.RequireOAuth {
			klog.Errorf("failed to get kubeconfig: %v", err)
			return nil, errors.New("failed to get kubeconfig")
		}
		return &Kubernetes{manager: m}, nil
	}
	clientCmdApiConfig.AuthInfos = make(map[string]*clientcmdapi.AuthInfo)
	derived := &Kubernetes{manager: &Manager{
		clientCmdConfig: clientcmd.NewDefaultClientConfig(clientCmdApiConfig, nil),
		cfg:             derivedCfg,
		staticConfig:    m.staticConfig,
	}}
	derived.manager.accessControlClientSet, err = NewAccessControlClientset(derived.manager.cfg, derived.manager.staticConfig)
	if err != nil {
		if m.staticConfig.RequireOAuth {
			klog.Errorf("failed to get kubeconfig: %v", err)
			return nil, errors.New("failed to get kubeconfig")
		}
		return &Kubernetes{manager: m}, nil
	}
	derived.manager.discoveryClient = memory.NewMemCacheClient(derived.manager.accessControlClientSet.DiscoveryClient())
	derived.manager.accessControlRESTMapper = NewAccessControlRESTMapper(
		restmapper.NewDeferredDiscoveryRESTMapper(derived.manager.discoveryClient),
		derived.manager.staticConfig,
	)
	derived.manager.dynamicClient, err = dynamic.NewForConfig(derived.manager.cfg)
	if err != nil {
		if m.staticConfig.RequireOAuth {
			klog.Errorf("failed to initialize dynamic client: %v", err)
			return nil, errors.New("failed to initialize dynamic client")
		}
		return &Kubernetes{manager: m}, nil
	}
	return derived, nil
}

func (k *Kubernetes) NewHelm() *helm.Helm {
	// This is a derived Kubernetes, so it already has the Helm initialized
	return helm.NewHelm(k.manager)
}
