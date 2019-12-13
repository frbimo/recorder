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
<!-- 
Check location first to find URI
If not found return 507
 -->
<!-- set limit ENV for limit instance default is 40. 41 return 507-->
--------------------
<!-- Check DB 1 by 1, not random -->
<!-- DBs:=[]string{"a","b","c"} -->
<!-- recorder will check a to c until found a vacant db, if not return 507-->
---------------------
<!-- 30 days can be set from ENV -->
<!-- each days query @4 o'clock -->
--------------------
<!-- if instance+id is the samee and space_id is not the same return 409 -->
<!-- if instance id and space is the same return 201 -->
if the same

max_days_dbdb
DATACENTER
<!-- empty ENV has uri, so we can query based on ENV -->
if query bsaed on env return empty, return 507

--------------------