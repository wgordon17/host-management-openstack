package management

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/baremetal/v1/nodes"
	"github.com/gophercloud/utils/v2/openstack/clientconfig"
)

var (
	_ Client        = (*OpenStackClient)(nil)
	_ NewClientFunc = NewClientFunc(NewOpenStackClient)
)

func init() {
	newClientFuncs["openstack"] = NewOpenStackClient
}

type OpenStackClient struct {
	client *gophercloud.ServiceClient
}

func NewOpenStackClient(ctx context.Context, cfg *Config) (Client, error) {
	var cloud clientconfig.Cloud
	if cfg != nil && cfg.Options != nil {
		if openstackOpts, ok := cfg.Options["openstack"]; ok {
			openstackOptsJSON, err := json.Marshal(openstackOpts)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal openstack options (check cloud configuration)")
			}
			if err := json.Unmarshal(openstackOptsJSON, &cloud); err != nil {
				return nil, fmt.Errorf("failed to unmarshal openstack options (check cloud configuration)")
			}
		}
	}

	clientOpts := clientconfig.ClientOpts{
		Cloud:        cloud.Cloud,
		AuthType:     cloud.AuthType,
		AuthInfo:     cloud.AuthInfo,
		RegionName:   cloud.RegionName,
		EndpointType: cloud.EndpointType,
	}

	providerClient, err := clientconfig.AuthenticatedClient(ctx, &clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create authenticated client (check cloud credentials and endpoint configuration)")
	}

	ironicClient, err := openstack.NewBareMetalV1(providerClient, gophercloud.EndpointOpts{
		Region:       cloud.RegionName,
		Availability: gophercloud.Availability(cloud.EndpointType),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create baremetal client (check endpoint configuration and region)")
	}

	return &OpenStackClient{client: ironicClient}, nil
}

func (c *OpenStackClient) GetPowerState(ctx context.Context, hostID string) (*PowerStatus, error) {
	node, err := nodes.Get(ctx, c.client, hostID).Extract()
	if err != nil {
		return nil, fmt.Errorf("get node %s: %w", hostID, err)
	}

	state := PowerState(node.PowerState)
	switch state {
	case PowerOn, PowerOff:
	default:
		return nil, fmt.Errorf("node %s: unexpected power state %q", hostID, node.PowerState)
	}

	return &PowerStatus{
		State:           state,
		IsTransitioning: node.TargetPowerState != "",
	}, nil
}

func (c *OpenStackClient) SetPowerState(ctx context.Context, hostID string, target PowerState) error {
	switch target {
	case PowerOn, PowerOff:
	default:
		return fmt.Errorf("node %s: invalid target power state %q", hostID, target)
	}

	res := nodes.ChangePowerState(ctx, c.client, hostID, nodes.PowerStateOpts{
		Target: nodes.TargetPowerState(target),
	})
	if err := res.ExtractErr(); err != nil {
		if gophercloud.ResponseCodeIs(err, http.StatusConflict) {
			return fmt.Errorf("node %s: %w", hostID, ErrTransitioning)
		}
		return fmt.Errorf("failed to set power state on node %s: %w", hostID, err)
	}
	return nil
}
