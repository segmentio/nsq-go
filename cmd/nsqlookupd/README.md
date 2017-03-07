nsqlookupd
==========

**[EXPERIMENTAL]**

This implementation of nsqlookupd was intended to support consul as a distributed
datastore.
In practice, it turned out to be much harder to manage consul sessions to expire
keys, so it went abandonned and we implemented nsqlookup-proxy instead to support
a single lookup endpoint.
