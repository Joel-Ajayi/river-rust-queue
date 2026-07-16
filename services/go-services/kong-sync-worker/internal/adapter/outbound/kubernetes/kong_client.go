package kubernetes

import (
	"context"
	"fmt"
	"strings"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/core/port"
	"go.uber.org/zap"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type KongClient struct {
	client        dynamic.Interface
	cfg           *platform.Config
	pubKeyPEM     string
	log           *zap.Logger
	gvrConsumer   schema.GroupVersionResource
	gvrPlugin     schema.GroupVersionResource
	gvrCredential schema.GroupVersionResource
}

// compile time interface implementation check
var _ port.KongGateway = (*KongClient)(nil)

// NewKongClient initializes a dynamic Kubernetes client to interact with Kong CRDs
func NewKongClient(cfg *platform.Config, log *zap.Logger, pubKeyPEM string) (*KongClient, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		// Fallback to kubeconfig if running locally
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		configOverrides := &clientcmd.ConfigOverrides{}
		kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
		config, err = kubeConfig.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to load kubeconfig: %v", err)
		}
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %v", err)
	}

	return &KongClient{
		client:        dynamicClient,
		cfg:           cfg,
		pubKeyPEM:     pubKeyPEM,
		log:           log,
		gvrConsumer:   schema.GroupVersionResource{Group: cfg.KongAPIGroup, Version: cfg.KongAPIVersion, Resource: cfg.KongResourceConsumers},
		gvrPlugin:     schema.GroupVersionResource{Group: cfg.KongAPIGroup, Version: cfg.KongAPIVersion, Resource: cfg.KongResourcePlugins},
		gvrCredential: schema.GroupVersionResource{Group: cfg.KongAPIGroup, Version: cfg.KongAPIVersion, Resource: cfg.KongResourceCredentials},
	}, nil
}

func (c *KongClient) SyncConsumer(ctx context.Context, merchantID string) error {
	name := strings.ReplaceAll(strings.ToLower(merchantID), domain.CharUnderscore, domain.CharDash)
	consumer := &unstructured.Unstructured{
		Object: map[string]interface{}{
			domain.FieldAPIVersion: c.cfg.KongConfigAPIVersion,
			domain.FieldKind:       c.cfg.KongKindConsumer,
			domain.FieldMetadata: map[string]interface{}{
				domain.FieldName:      name,
				domain.FieldNamespace: c.cfg.KubernetesNamespace,
			},
			domain.FieldUsername: name,
			domain.FieldCustomID: merchantID,
		},
	}

	obj, err := c.client.Resource(c.gvrConsumer).Namespace(c.cfg.KubernetesNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = c.client.Resource(c.gvrConsumer).Namespace(c.cfg.KubernetesNamespace).Create(ctx, consumer, metav1.CreateOptions{})
			if err != nil {
				c.log.Error("Failed to create KongConsumer", zap.String("name", name), zap.Error(err))
				return err
			}
			c.log.Info("Created KongConsumer", zap.String("name", name))
		} else {
			return err
		}
	} else {
		obj.Object[domain.FieldUsername] = consumer.Object[domain.FieldUsername]
		obj.Object[domain.FieldCustomID] = consumer.Object[domain.FieldCustomID]
		_, err = c.client.Resource(c.gvrConsumer).Namespace(c.cfg.KubernetesNamespace).Update(ctx, obj, metav1.UpdateOptions{})
		if err != nil {
			c.log.Error("Failed to update KongConsumer", zap.String("name", name), zap.Error(err))
			return err
		}
	}
	return nil
}

func (c *KongClient) SyncRateLimitPlugin(ctx context.Context, merchantID, tier string) error {
	consumerName := strings.ReplaceAll(strings.ToLower(merchantID), domain.CharUnderscore, domain.CharDash)
	name := fmt.Sprintf(domain.FormatRateLimitName, consumerName)

	minuteLimit := domain.RateLimitStandard
	if tier == domain.TierPremium {
		minuteLimit = domain.RateLimitPremium
	}

	plugin := &unstructured.Unstructured{
		Object: map[string]interface{}{
			domain.FieldAPIVersion: c.cfg.KongConfigAPIVersion,
			domain.FieldKind:       c.cfg.KongKindPlugin,
			domain.FieldMetadata: map[string]interface{}{
				domain.FieldName:      name,
				domain.FieldNamespace: c.cfg.KubernetesNamespace,
			},
			domain.FieldPlugin: c.cfg.KongPluginRateLimiting,
			domain.FieldConsumerRef: map[string]interface{}{
				domain.FieldName: consumerName,
			},
			domain.FieldConfig: map[string]interface{}{
				domain.FieldMinute: minuteLimit,
				domain.FieldPolicy: c.cfg.KongPluginPolicy,
			},
		},
	}

	obj, err := c.client.Resource(c.gvrPlugin).Namespace(c.cfg.KubernetesNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = c.client.Resource(c.gvrPlugin).Namespace(c.cfg.KubernetesNamespace).Create(ctx, plugin, metav1.CreateOptions{})
			if err != nil {
				c.log.Error("Failed to create KongPlugin", zap.String("name", name), zap.Error(err))
				return err
			}
			c.log.Info("Created KongPlugin", zap.String("name", name))
		} else {
			return err
		}
	} else {
		obj.Object[domain.FieldConfig] = plugin.Object[domain.FieldConfig]
		_, err = c.client.Resource(c.gvrPlugin).Namespace(c.cfg.KubernetesNamespace).Update(ctx, obj, metav1.UpdateOptions{})
		if err != nil {
			c.log.Error("Failed to update KongPlugin", zap.String("name", name), zap.Error(err))
			return err
		}
	}

	return nil
}

func (c *KongClient) SyncJWTCredential(ctx context.Context, merchantID string) error {
	consumerName := strings.ReplaceAll(strings.ToLower(merchantID), domain.CharUnderscore, domain.CharDash)
	name := fmt.Sprintf(domain.FormatJWTName, consumerName)

	credential := &unstructured.Unstructured{
		Object: map[string]interface{}{
			domain.FieldAPIVersion: c.cfg.KongConfigAPIVersion,
			domain.FieldKind:       c.cfg.KongKindCredential,
			domain.FieldMetadata: map[string]interface{}{
				domain.FieldName:      name,
				domain.FieldNamespace: c.cfg.KubernetesNamespace,
			},
			domain.FieldType: c.cfg.KongCredentialTypeJWT,
			domain.FieldConsumerRef: map[string]interface{}{
				domain.FieldName: consumerName,
			},
			domain.FieldConfig: map[string]interface{}{
				domain.FieldAlgorithm:    c.cfg.KongJWTAlgorithm,
				domain.FieldKey:          merchantID, // The 'iss' claim (or 'key' header) they must send
				domain.FieldRSAPublicKey: c.pubKeyPEM,
			},
		},
	}

	obj, err := c.client.Resource(c.gvrCredential).Namespace(c.cfg.KubernetesNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = c.client.Resource(c.gvrCredential).Namespace(c.cfg.KubernetesNamespace).Create(ctx, credential, metav1.CreateOptions{})
			if err != nil {
				c.log.Error("Failed to create KongCredential", zap.String("name", name), zap.Error(err))
				return err
			}
			c.log.Info("Created KongCredential", zap.String("name", name))
		} else {
			return err
		}
	} else {
		obj.Object[domain.FieldConfig] = credential.Object[domain.FieldConfig]
		_, err = c.client.Resource(c.gvrCredential).Namespace(c.cfg.KubernetesNamespace).Update(ctx, obj, metav1.UpdateOptions{})
		if err != nil {
			c.log.Error("Failed to update KongCredential", zap.String("name", name), zap.Error(err))
			return err
		}
	}

	return nil
}
