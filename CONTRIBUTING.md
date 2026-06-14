# Contributing to CacheX

Thank you for your interest in contributing to CacheX! This document provides guidelines and instructions for contributing.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/cachex.git`
3. Add upstream: `git remote add upstream https://github.com/gospacex/cachex.git`

## Development Setup

```bash
# Install dependencies
go mod download

# Run tests
make test

# Run with coverage
make test-cover

# Run linter
make lint-ci
```

## Making Changes

1. Create a feature branch: `git checkout -b feature/your-feature-name`
2. Make your changes
3. Add tests for your changes
4. Ensure all tests pass: `make test`
5. Format code: `make fmt`
6. Commit your changes: `git commit -m 'Add feature: description'`
7. Push to your fork: `git push origin feature/your-feature-name`
8. Open a Pull Request

## Code Style

- Follow Go's standard formatting (`go fmt`)
- Use meaningful variable names
- Add comments for complex logic
- Write godoc comments for public functions

## Testing Guidelines

- All new features must include tests
- All tests must pass before merging
- Aim for 80%+ code coverage
- Run race detector: `make test-race`

## Pull Request Process

1. Update documentation if needed
2. Add tests for new functionality
3. Update README.md if applicable
4. The PR will be reviewed by maintainers
5. Once approved, it will be merged

## Reporting Issues

When reporting issues, please include:

- Go version
- CacheX version
- Backend type and version
- Minimal reproduction case
- Expected vs actual behavior

## Questions?

Feel free to open an issue for any questions or discussions.