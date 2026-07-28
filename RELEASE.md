[Aerospike Shared-Memory Tool (ASMT)](https://docs.aerospike.com/tools/asmt) supports faster cold starts of nodes in an Aerospike Database Enterprise Edition (EE) cluster. See the new docs for more information.

## Improvements
* [TOOLS-3178] - (ASMT) Drop support for Ubuntu 20.04.
* [TOOLS-3552] - (ASMT) Add support for Ubuntu 26.04.

## Bug Fixes
* [TOOLS-4136] - (ASMT) Misclassifies 'ad' key-base data segments as TreeX, silently skipping them on restore ("Missing treex segment file").
