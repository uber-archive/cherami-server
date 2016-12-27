This is the directory which holds all the common stuff required by
all the different services.

The structure within this is folder is something like this:

config.go - holds all the config items
util.go   - holds the utlity stuff like creating tchannel, boot strapping
            ringpop, etc.
*types.go - holds the types for the specific interface
            (eg: servicetypes.go - holds the object and interface
             definition for all common stuff used by our services)
*.go      - business logic implementing the interface
            (eg: service.go - implements the methods specific to
            servicetypes.go)
