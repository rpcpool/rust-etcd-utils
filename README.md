# rust-etcd-utils


Utility library for common ETCD management in Rust, it covers:

1. Automatic lease management.
2. Automatic lock lifecycle managemented: auto-revoke and auto-keep-alive.
3. Builtin retry logic for every public function to make robust in-production application.

## Add to library

```
cargo add rust-etcd-utils
```


## How to test

Uses `compose.yaml` to launch and instance of `etcd` with port-fowarding over the port 2379, so `localhost:2379` redirects to `etcd` instance inside docker.

Then run the following command

```sh
$ cargo test --test -- --nocapture
```