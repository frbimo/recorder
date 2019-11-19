package recorder

import (
	osb "github.com/pmorie/go-open-service-broker-client/v2"
	"github.com/pmorie/osb-broker-lib/pkg/broker"
)

type (

	// ObjectProvision is an object decorator for provision
	ObjectProvision func(request *osb.ProvisionRequest, c *broker.RequestContext) (*broker.ProvisionResponse, error)

	// ObjectDeprovision is an object decorator for deprovision
	ObjectDeprovision func(request *osb.DeprovisionRequest, c *broker.RequestContext) (*broker.DeprovisionResponse, error)

	// ObjectBind is an object decorator for bind
	ObjectBind func(request *osb.BindRequest, c *broker.RequestContext) (*broker.BindResponse, error)

	// ObjectUnbind is an object decorator for unbind
	ObjectUnbind func(request *osb.UnbindRequest, c *broker.RequestContext) (*broker.UnbindResponse, error)
)
