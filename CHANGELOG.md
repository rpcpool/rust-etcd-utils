# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Version 0 of Semantic Versioning is handled differently from version 1 and above.
The minor version will be incremented upon a breaking change and the patch version will be incremented for features.

## [Unreleased]

### Features

## [0.13.0]

### Breaking

- `tonic` upgrade to 0.14.x
- `etcd-client` upgrade to  0.17.x

## [0.12.0]

### Breaking

- tonic update to 0.13

## [0.11.0]

### Breaking

- Renamed `spawn_lock_manager` to `spawn_lock_manager_with_lease_man`.
- Replaced `ManagedLease::new` with `ManagedLease::spawn` and `ManagedLease::spawn_on` as it better indicates the intent the the user.

### Changes

- Refactored the `ManagedLease` implementation to use a background task to handle lease creation and lifecycle management.
Removing the need to use `Arc<Mutex<...>>`.

## [0.10.0]

- Removed pinned dependencies for "^" in mature dependencies and "~" for non-mature deps.