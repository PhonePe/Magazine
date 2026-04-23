# Contributing to Magazine

Thank you for considering contributing to Magazine! This guide explains the process for contributing to this project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Reporting Issues](#reporting-issues)

## Code of Conduct

This project adheres to the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to [opensource@phonepe.com](mailto:opensource@phonepe.com).

## Getting Started

1. **Fork** the repository on GitHub.
2. **Clone** your fork locally:
   ```bash
   git clone https://github.com/<your-username>/Magazine.git
   cd Magazine
   ```
3. **Add the upstream remote:**
   ```bash
   git remote add upstream https://github.com/PhonePe/Magazine.git
   ```

## Development Setup

### Prerequisites

- **Java 17** or later
- **Apache Maven 3.8+**
- **Docker** (for integration tests using Testcontainers)

### Build

```bash
mvn clean install
```

### Run Tests

```bash
mvn test
```

> **Note:** Integration tests require Docker to be running, as they use Testcontainers to spin up an Aerospike instance.

### Generate Javadoc

```bash
mvn javadoc:javadoc
```

## Making Changes

1. Create a feature branch from `main`:
   ```bash
   git checkout -b feature/my-feature
   ```
2. Make your changes in small, focused commits.
3. Write or update tests for your changes.
4. Ensure all tests pass:
   ```bash
   mvn clean verify
   ```
5. Update documentation if your changes affect the public API.

## Pull Request Process

1. Push your branch to your fork:
   ```bash
   git push origin feature/my-feature
   ```
2. Open a Pull Request against the `main` branch of the upstream repository.
3. Fill in the PR template with:
   - A clear description of the change
   - The motivation / issue being addressed
   - Steps to test the change
4. Ensure the CI build passes.
5. Request review from at least one maintainer.
6. Address review feedback by pushing additional commits.
7. Once approved, a maintainer will merge your PR.

## Coding Standards

- **Language level:** Java 17 (use pattern matching, records, sealed classes where appropriate).
- **Formatting:** Follow the existing code style. Lombok annotations are used throughout—keep it consistent.
- **Naming:** Use clear, descriptive names. Prefix test methods with `test` or use descriptive `should_X_when_Y` naming.
- **Documentation:** Add Javadoc to all public classes and methods.
- **Testing:**
  - Unit tests with JUnit 4 and Mockito.
  - Integration tests with Testcontainers for backend-specific logic.
  - Aim for meaningful coverage; don't just chase numbers.
- **Dependencies:** Avoid adding new dependencies unless absolutely necessary. Discuss in the issue first.
- **Commits:** Write clear commit messages. Use the imperative mood ("Add feature" not "Added feature").

## Reporting Issues

- Use [GitHub Issues](https://github.com/PhonePe/Magazine/issues) to report bugs or request features.
- Include:
  - A clear title and description
  - Steps to reproduce (for bugs)
  - Expected vs. actual behavior
  - Magazine version, Java version, and backend version
  - Relevant logs or stack traces

## License

By contributing to Magazine, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).

