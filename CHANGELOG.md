# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added
- Unified Cache interface for all backends
- Redis backend with cluster support
- Dragonfly backend support
- KeyDB backend support
- Garnet backend support
- Badger embedded backend
- BBolt embedded backend
- Pebble embedded backend
- Prometheus metrics collection
- Circuit breaker pattern
- Structured logging
- Rate limiting extension
- Distributed lock extension
- Bloom filter extension
- Health check support
- TLS connection support
- Environment variable configuration
- Comprehensive test suite
- CI/CD pipeline with GitHub Actions

### Changed
- Improved error handling with wrapped errors
- Enhanced configuration validation

### Fixed
- Proper connection pool management
- Graceful shutdown handling

## [1.0.0] - 2026-06-04

### Added
- Initial release
- Core cache interface
- Basic backend implementations