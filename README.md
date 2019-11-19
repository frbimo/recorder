# Recorder

[![Go Report Card](https://goreportcard.com/badge/github.com/frbimo/recorder)](https://goreportcard.com/report/github.com/frbimo/recorder)

Recorder is a library for store ops log into ops database for adv. 


## Example: How to integrate service broker function with Recorder

### Your logic function
```go
import (
    osb "github.com/pmorie/go-open-service-broker-client/v2"
    broker "github.com/pmorie/osb-broker-lib/pkg/"
)

type MyLogic struct {
    // internal state goes here
}

func (l *MyLogic) Provision(request *osb.ProvisionRequest, c *broker.RequestContext) (*broker.ProvisionResponse, error)  {
//    your Provision logic here

    return &broker.ProvisionResponse{}, nil
}
```

### Using Recorder
```go
import (
    osb "github.com/pmorie/go-open-service-broker-client/v2"
    broker "github.com/pmorie/osb-broker-lib/pkg/"
    "github.com/frbimo/recorder"

    sm "<your-package-url>"
)

type BrokerLogic struct {
    // internal state goes here
}

func (b *BrokerLogic) Provision(request *osb.ProvisionRequest, c *broker.RequestContext) (*broker.ProvisionResponse, error)  {

  	// Initiate new recorder
    rec := recorder.New()
    defer rec.Cli.Close()
    
    // Put your login function in recorder 
    fn := rec.OnProvision(sm.Provision)
    
    // Handle the return of your logic function
    resp, err := fn(request, c)

    if err != nil {
      return nil, err
    }

    if request.AcceptsIncomplete {
      resp.Async = b.async
    }

    return resp, nil
}
```